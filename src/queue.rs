//! `SeekSage`: бесблокиционный транспорт между потоками.
//!
//! Модуль содержит две структуры, отработанные в наших бенчмарках:
//!
//! * [`SeqRing`] — кольцо на схеме **seq-per-slot**: каждый слот ровно одна
//!   кэш-линия (64 байта), атомарный маркер `seq` и payload лежат в одной
//!   `#[repr(align(64))]` структуре, то есть на одной линии.
//! * [`Lanes`] — шардированная MPSC-очередь (много производителей, один
//!   потребитель) из независимых SPSC-дорожек: каждый пишущий поток имеет
//!   монопольный доступ к своей дорожке, единственный потребитель циклически
//!   опрашивает дорожки по очереди. Управляющие индексы `head`/`tail` обёрнуты
//!   в [`Padded`] с `#[repr(align(64))]`.
//!
//! # Память и порядок (ядро lock-free гарантий)
//!
//! Вся синхронизация — только Acquire/Release на атомарных маркерах, без
//! мьютексов, без CAS в горячем пути, без Vec:
//!
//! * производитель пишет payload, затем делает **Release-store** маркера
//!   (`slot.seq` / `lane.tail`) — все записи payload видны тому, кто
//!   acquire-прочитал маркер;
//! * потребитель делает **Acquire-load** маркера, только после этого читает
//!   payload;
//! * потребитель освобождает позицию и делает **Release-store** индекса
//!   освобождения (`slot.seq` / `lane.head`), производитель Acquire-грузит его
//!   перед записью в слот — так producer никогда не затирает ещё не дочитанные
//!   данные.
//!
//! Индексы `write_index` / `read_index` приватны для каждой стороны и растут
//! монотонно; в слот они проецируются через `mask` (ёмкость — степень двойки).
//! Монотонный `usize` обёртывается на 2^64, и обёрт безопасен: `cap` — степень
//! двойки, поэтому `index & mask` после обёрта продолжается последовательно, а
//! значения маркера `seq` никогда не повторяются — ABA-проблемы нет, и обёрт
//! физически недостижим (2^64 записей).
//!
//! Сырая аллокация под 64-байтные структуры возвращается `alloc` как `*mut u8`;
//! cast к `*mut SeqSlot` / `*mut LaneSlot` / `*mut Lane` безопасен, потому что
//! layout запрашивается с выравниванием 64 — clippy этого не выводит, отсюда
//! allow на уровне модуля.
#![allow(clippy::cast_ptr_alignment)]

use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Кэш-линия современного ядра x86-64 / ARM64.
pub const CACHE_LINE: usize = 64;

/// Payload одного слота `SeqSlot`: 64 - 8 (seq) - 8 (len) = 48 байт.
pub const SEQ_SLOT_PAYLOAD: usize = CACHE_LINE - 2 * core::mem::size_of::<usize>();

/// Payload одного слота дорожки: 64 - 8 (len) = 56 байт.
pub const LANE_SLOT_PAYLOAD: usize = CACHE_LINE - core::mem::size_of::<usize>();

// ---------------------------------------------------------------------------
// Padded: управляющие индексы на отдельных кэш-линиях
// ---------------------------------------------------------------------------

/// Счётчик индекса, занимающий ровно свою кэш-линию.
///
/// Producer трогает только `tail`, consumer — только `head`; разведя их по
/// разным 64-байтным линиям, мы снимаем false sharing между ядрами.
#[repr(align(64))]
pub struct Padded {
    value: AtomicUsize,
    _pad: [u8; CACHE_LINE - core::mem::size_of::<AtomicUsize>()],
}

impl Padded {
    #[inline]
    #[must_use]
    pub fn new(value: usize) -> Self {
        Self {
            value: AtomicUsize::new(value),
            _pad: [0u8; CACHE_LINE - core::mem::size_of::<AtomicUsize>()],
        }
    }

    /// Acquire-load: увидеть данные, «опубликованные» парным release-store.
    #[inline]
    #[must_use]
    pub fn load(&self) -> usize {
        self.value.load(Ordering::Acquire)
    }

    /// Release-store: все записи, сделанные до него, становятся видны стороне,
    /// которая Acquire-грузит это значение.
    #[inline]
    pub fn store(&self, value: usize) {
        self.value.store(value, Ordering::Release);
    }
}

const _: () = assert!(core::mem::size_of::<Padded>() == CACHE_LINE);
const _: () = assert!(core::mem::align_of::<Padded>() == CACHE_LINE);

// ---------------------------------------------------------------------------
// SeqRing — seq-per-slot
// ---------------------------------------------------------------------------

/// Внутреннее содержимое слота: маркер + длина + payload, ровно 64 байта.
struct SeqData {
    /// Маркер раунда слота.
    ///
    /// `seq == i` — слот свободен и готов принять запись при `write_index == i`.
    /// `seq == i + 1` — в слоте лежат данные, записанные при `write_index == i`.
    seq: AtomicUsize,
    /// Длина payload. Пишется **до** release-store `seq`, читается **после**
    /// acquire-load `seq`, поэтому видимость гарантирована парой Release/Acquire.
    len: usize,
    data: [u8; SEQ_SLOT_PAYLOAD],
}

/// Слот = ровно одна кэш-линия: маркер состояния и данные вместе.
#[repr(align(64))]
pub(crate) struct SeqSlot {
    inner: UnsafeCell<SeqData>,
}

impl SeqSlot {
    #[inline]
    fn new(seq: usize) -> Self {
        Self {
            inner: UnsafeCell::new(SeqData {
                seq: AtomicUsize::new(seq),
                len: 0,
                data: [0u8; SEQ_SLOT_PAYLOAD],
            }),
        }
    }

    /// Acquire-load маркера слота.
    #[inline]
    fn seq_acquire(&self) -> usize {
        unsafe { (*self.inner.get()).seq.load(Ordering::Acquire) }
    }

    /// Release-store маркера слота.
    #[inline]
    fn seq_release(&self, value: usize) {
        unsafe { (*self.inner.get()).seq.store(value, Ordering::Release) }
    }

    /// Прочитать payload (длина опубликована acquire-load маркера).
    #[inline]
    fn payload(&self) -> &[u8] {
        unsafe {
            let d = &*self.inner.get();
            &d.data[..d.len]
        }
    }

    /// Заполнить payload; вызывается только когда слот закреплён за производителем.
    #[inline]
    fn fill(&self, text: &[u8]) {
        unsafe {
            let d = &mut *self.inner.get();
            d.len = text.len();
            core::ptr::copy_nonoverlapping(text.as_ptr(), d.data.as_mut_ptr(), text.len());
        }
    }
}

const _: () = assert!(core::mem::size_of::<SeqSlot>() == CACHE_LINE);
const _: () = assert!(core::mem::align_of::<SeqSlot>() == CACHE_LINE);

/// Кольцо seq-per-slot: один производитель и один потребитель.
///
/// Ёмкость округляется до степени двойки, аллокация — сырая, выровненная по
/// 64 байта: `cap` слотов подряд, каждый слот — своя кэш-линия.
pub struct SeqRing {
    slots: NonNull<SeqSlot>,
    cap: usize,
    mask: usize,
    /// Жетон производителя выдан один раз; флаг трогается только при создании
    /// жетона, в горячем пути атомик не участвует.
    producer_claimed: AtomicBool,
    consumer_claimed: AtomicBool,
}

impl SeqRing {
    /// Выделить `cap` слотов и проставить каждому начальный маркер `i`
    /// (слот `i` готов к первой записи при `write_index == i`).
    ///
    /// # Panics
    ///
    /// Если округление `cap` до степени двойки переполняет `usize`
    /// (`next_power_of_two`) или layout массива слотов переполняет `usize`.
    /// При нехватке памяти процесс завершается через `handle_alloc_error`, как
    /// того требует `alloc`.
    #[inline]
    #[must_use]
    pub fn new(cap: usize) -> Self {
        let cap = cap.max(2).next_power_of_two();
        let layout = Layout::array::<SeqSlot>(cap).expect("layout overflow");
        // SAFETY: layout с выравниванием 64 и размером cap * 64; память
        // инициализируется по слоту через ptr::write ниже, обнуление тут —
        // чистый оверхед, поэтому обычный alloc.
        let raw = unsafe { alloc(layout).cast::<SeqSlot>() };
        if raw.is_null() {
            handle_alloc_error(layout);
        }
        for i in 0..cap {
            // SAFETY: слот инициализируется значением, которое перемещается в
            // выделенную память; SeqSlot не имеет drop glue.
            unsafe { core::ptr::write(raw.add(i), SeqSlot::new(i)) };
        }
        // SAFETY: alloc вернул ненулевой указатель (проверено выше).
        Self {
            slots: unsafe { NonNull::new_unchecked(raw) },
            cap,
            mask: cap - 1,
            producer_claimed: AtomicBool::new(false),
            consumer_claimed: AtomicBool::new(false),
        }
    }

    /// Жетон производителя; жетон закреплён за одним потоком.
    ///
    /// Возвращает `None`, если производитель уже выдан: два жетона на одном
    /// кольце разрушили бы lock-free схему (два приватных `write_index` писали
    /// бы в один слот). CAS только здесь, в горячем пути его нет.
    pub fn producer(&self) -> Option<SeqProducer<'_>> {
        // SAFETY: флаг живёт внутри кольца; AcqRel связывает выдачу жетона со
        // всеми последующими acquire/release на маркерах слотов.
        if self
            .producer_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            Some(SeqProducer {
                ring: self,
                write_index: 0,
            })
        } else {
            None
        }
    }

    /// Жетон потребителя; жетон закреплён за одним потоком.
    ///
    /// Возвращает `None`, если потребитель уже выдан.
    pub fn consumer(&self) -> Option<SeqConsumer<'_>> {
        if self
            .consumer_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            Some(SeqConsumer {
                ring: self,
                read_index: 0,
            })
        } else {
            None
        }
    }

    /// Количество реальных слотов кольца.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Указатель на слот для монотонного индекса `index`.
    #[inline]
    fn slot(&self, index: usize) -> &SeqSlot {
        unsafe { &*self.slots.as_ptr().add(index & self.mask) }
    }
}

impl Drop for SeqRing {
    fn drop(&mut self) {
        // SAFETY: память выделена этим же layout; SeqSlot не требует drop glue.
        unsafe {
            let layout = Layout::array::<SeqSlot>(self.cap).expect("layout overflow");
            dealloc(self.slots.as_ptr().cast::<u8>(), layout);
        }
    }
}

// SAFETY: SeqRing — единый блок выровненной памяти из атомарных слотов;
// межпоточное взаимодействие идёт исключительно через пары Release/Acquire на
// `seq`, поэтому кольцо синхронизируется само. NonNull<SeqSlot> — это !Send и
// !Sync по std, оба impl нужны: Send за счёт того, что жетон производителя
// закреплён за одним потоком, Sync — из-за UnsafeCell в SeqSlot.
unsafe impl Sync for SeqRing {}
unsafe impl Send for SeqRing {}

/// Некопируемый и неклонируемый жетон производителя `SeqRing`.
///
/// Ровно один поток владеет жетоном, значит `write_index` приватен и CAS не
/// нужен. Методы никогда не блокируются: при отставании потребителя
/// `publish` просто возвращает `None`.
pub struct SeqProducer<'r> {
    ring: &'r SeqRing,
    write_index: usize,
}

impl SeqProducer<'_> {
    /// Есть ли свободный слот под текущим `write_index` (Acquire-load маркера).
    #[inline]
    #[must_use]
    pub fn ready(&self) -> bool {
        self.ring.slot(self.write_index).seq_acquire() == self.write_index
    }

    /// Опубликовать строку/байты в кольцо.
    ///
    /// Возвращает `Some(monotонный номер публикации)` (1, 2, 3, ...) при успехе
    /// и `None`, если кольцо забито (потребитель не освободил слот) либо payload
    /// длиннее [`SEQ_SLOT_PAYLOAD`]. Блокировки и CAS нет.
    #[inline]
    pub fn publish(&mut self, text: &[u8]) -> Option<usize> {
        if text.len() > SEQ_SLOT_PAYLOAD {
            return None;
        }
        let slot = self.ring.slot(self.write_index);
        // Acquire: убеждаемся, что потребитель предыдущего раунда отпустил слот.
        if slot.seq_acquire() != self.write_index {
            return None;
        }
        slot.fill(text);
        // Release: payload опубликован, слот готов к чтению при write_index == i.
        slot.seq_release(self.write_index + 1);
        self.write_index += 1;
        Some(self.write_index)
    }

    /// То же, что [`publish`](Self::publish), но для `&str`.
    #[inline]
    pub fn publish_str(&mut self, s: &str) -> Option<usize> {
        self.publish(s.as_bytes())
    }

    /// Сколько элементов опубликовано этим производителем.
    #[inline]
    #[must_use]
    pub fn written(&self) -> usize {
        self.write_index
    }
}

/// Guard слота: освобождение маркера происходит и при нормальном выходе из
/// итерации, и при панике внутри callback. Иначе слот навсегда останется
/// «занимаемым» (маркер `r + 1`), и кольцо заклинит на раунде `r + cap`.
struct SeqReleaseGuard<'r> {
    slot: &'r SeqSlot,
    release: usize,
}

impl Drop for SeqReleaseGuard<'_> {
    fn drop(&mut self) {
        self.slot.seq_release(self.release);
    }
}

/// Некопируемый и неклонируемый жетон потребителя `SeqRing`.
pub struct SeqConsumer<'r> {
    ring: &'r SeqRing,
    read_index: usize,
}

impl SeqConsumer<'_> {
    /// Есть ли данные под текущим `read_index` (Acquire-load маркера).
    #[inline]
    #[must_use]
    pub fn ready(&self) -> bool {
        self.ring.slot(self.read_index).seq_acquire() == self.read_index + 1
    }

    /// Вычерпать все доступные payload подряд, отдавая каждый слайс в `on_chunk`.
    ///
    /// Возвращает число вычерпанных элементов. После передачи payload в callback
    /// слот освобождается release-store маркера, и производитель может писать в
    /// него в следующем раунде. Слайс, переданный в callback, живёт только
    /// внутри вызова — сохранённый слайс производитель может перетереть в
    /// следующем раунде, поэтому забирать его из callback нельзя.
    ///
    /// Освобождение слота делает guard, поэтому паника в `on_chunk` не клинит
    /// кольцо: слот отпускается при распаковке стека. Раунд, на котором
    /// callback паниковал, потребитель пропускает — маркер слота уже
    /// переставлен на `read_index + cap`, и без пропуска потребитель стоял бы
    /// на этом раунде навсегда.
    #[inline]
    pub fn drain(&mut self, mut on_chunk: impl FnMut(&[u8])) -> usize {
        let mut n = 0;
        loop {
            let slot = self.ring.slot(self.read_index);
            let seq = slot.seq_acquire();
            if seq == self.read_index + self.ring.cap {
                // Раунд этого элемента уже отпущен: он был пропущен (паника
                // callback) — идём дальше, кольцо продолжает работать.
                self.read_index += 1;
                continue;
            }
            if seq != self.read_index + 1 {
                break;
            }
            // Освобождаем слот: маркер становится индексом будущей записи в него.
            let _guard = SeqReleaseGuard {
                slot,
                release: self.read_index + self.ring.cap,
            };
            on_chunk(slot.payload());
            self.read_index += 1;
            n += 1;
        }
        n
    }

    /// Сколько элементов потреблено этим потребителем.
    #[inline]
    #[must_use]
    pub fn consumed(&self) -> usize {
        self.read_index
    }
}

// ---------------------------------------------------------------------------
// Lanes — шардированная MPSC из независимых SPSC-дорожек
// ---------------------------------------------------------------------------

/// Внутреннее содержимое слота дорожки: len + payload, 64 байта.
struct LaneData {
    len: usize,
    data: [u8; LANE_SLOT_PAYLOAD],
}

/// Слот дорожки = ровно одна кэш-линия; соседние слоты не делят линию, так что
/// producer и consumer не дёргают одну линию при движении индексов.
#[repr(align(64))]
pub(crate) struct LaneSlot {
    inner: UnsafeCell<LaneData>,
}

impl LaneSlot {
    #[inline]
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(LaneData {
                len: 0,
                data: [0u8; LANE_SLOT_PAYLOAD],
            }),
        }
    }

    #[inline]
    fn payload(&self) -> &[u8] {
        unsafe {
            let d = &*self.inner.get();
            &d.data[..d.len]
        }
    }

    #[inline]
    fn fill(&self, text: &[u8]) {
        unsafe {
            let d = &mut *self.inner.get();
            d.len = text.len();
            core::ptr::copy_nonoverlapping(text.as_ptr(), d.data.as_mut_ptr(), text.len());
        }
    }
}

const _: () = assert!(core::mem::size_of::<LaneSlot>() == CACHE_LINE);
const _: () = assert!(core::mem::align_of::<LaneSlot>() == CACHE_LINE);

/// Одна SPSC-дорожка: приватный `tail` производителя, приватный `head`
/// потребителя, массив слотов. Контеншена между ядрами нет: оба индекса
/// лежат на отдельных кэш-линиях, а слоты — в своей памяти.
///
/// `repr(C)` обязателен: вся схема разведения счётчиков по кэш-линиям опирается
/// на задекларированный порядок полей (`tail` на offset 0, `head` на offset 64),
/// а `repr(Rust)` порядок полей не гарантирует.
#[repr(C, align(64))]
pub(crate) struct Lane {
    tail: Padded,
    head: Padded,
    slots: NonNull<LaneSlot>,
    cap: usize,
    mask: usize,
    /// Монопольный производитель дорожки выдан один раз. Флаг трогается только
    /// при выдаче жетона, в горячем пути он не участвует.
    claimed: AtomicBool,
}

const _: () = assert!(core::mem::align_of::<Lane>() == CACHE_LINE);
// tail (64) + head (64) + slots/cap/mask/claimed (64) = ровно 3 кэш-линии.
const _: () = assert!(core::mem::size_of::<Lane>() == 3 * CACHE_LINE);

/// Массив независимых дорожек. MPSC-очередь собрана из SPSC-дорожек: пишущий
/// поток `i` имеет монопольный жетон дорожки `i`, единственный потребитель
/// опрашивает дорожки циклически.
pub struct Lanes {
    /// Массив дорожек, выровненный по 64 байта; каждая дорожка — 3 линии.
    shards: NonNull<Lane>,
    lane_count: usize,
    lane_cap: usize,
    /// Жетон единственного потребителя выдан один раз; флаг трогается только
    /// при выдаче.
    consumer_claimed: AtomicBool,
}

impl Lanes {
    /// Создать `lane_count` дорожек; ёмкость каждой округляется до степени двойки.
    ///
    /// При нехватке памяти для слотов дорожки `i` уже построенные дорожки
    /// освобождаются перед завершением процесса через `handle_alloc_error`:
    /// процесс абортится, так что откат видит аллокатор, а не наблюдатель.
    ///
    /// # Panics
    ///
    /// Если `lane_count == 0` (нужна хотя бы одна дорожка); если округление
    /// `lane_cap` до степени двойки переполняет `usize` (`next_power_of_two`);
    /// если layout массива дорожек или массива слотов переполняет `usize`.
    #[inline]
    #[must_use]
    pub fn new(lane_count: usize, lane_cap: usize) -> Self {
        assert!(lane_count > 0, "нужна хотя бы одна дорожка");
        let lane_cap = lane_cap.max(2).next_power_of_two();
        let slot_layout = Layout::array::<LaneSlot>(lane_cap).expect("layout overflow");
        let lane_layout = Layout::array::<Lane>(lane_count).expect("layout overflow");

        let lanes_raw = unsafe { alloc(lane_layout).cast::<Lane>() };
        if lanes_raw.is_null() {
            handle_alloc_error(lane_layout);
        }

        for i in 0..lane_count {
            // SAFETY: массив слотов дорожки, выровненный по 64 байта; обнуление
            // здесь — чистый оверхед, каждый слот пишется через ptr::write.
            let slots_raw = unsafe { alloc(slot_layout).cast::<LaneSlot>() };
            if slots_raw.is_null() {
                // SAFETY: откат — освобождаем слоты уже построенных дорожек и
                // сам массив дорожек; значения Lane читаются по битам, drop glue
                // у Lane нет.
                unsafe {
                    for j in 0..i {
                        let p = lanes_raw.add(j).read().slots;
                        dealloc(p.as_ptr().cast::<u8>(), slot_layout);
                    }
                    dealloc(lanes_raw.cast::<u8>(), lane_layout);
                }
                handle_alloc_error(slot_layout);
            }
            for k in 0..lane_cap {
                // SAFETY: слот инициализируется на месте; drop glue отсутствует.
                unsafe { core::ptr::write(slots_raw.add(k), LaneSlot::new()) };
            }
            // SAFETY: NonNull проверен выше.
            let slots = unsafe { NonNull::new_unchecked(slots_raw) };
            // SAFETY: пишем Lane в выделенную, но ещё не инициализированную память.
            unsafe {
                core::ptr::write(
                    lanes_raw.add(i),
                    Lane {
                        tail: Padded::new(0),
                        head: Padded::new(0),
                        slots,
                        cap: lane_cap,
                        mask: lane_cap - 1,
                        claimed: AtomicBool::new(false),
                    },
                );
            }
        }

        Self {
            // SAFETY: alloc вернул ненулевой указатель (проверено выше).
            shards: unsafe { NonNull::new_unchecked(lanes_raw) },
            lane_count,
            lane_cap,
            consumer_claimed: AtomicBool::new(false),
        }
    }

    /// Монопольный жетон производителя дорожки `index`.
    ///
    /// Возвращает `None`, если дорожка уже закреплена за другим потоком:
    /// два производителя на одной дорожке разрушили бы SPSC-схему (два приватных
    /// `write_index` писали бы в один слот). CAS только при выдаче жетона.
    ///
    /// # Panics
    ///
    /// Если `index >= lane_count`.
    pub fn lane(&self, index: usize) -> Option<LaneProducer<'_>> {
        assert!(index < self.lane_count, "дорожка вне диапазона");
        // SAFETY: массив дорожек инициализирован в new(); жетон привязан
        // жизненным параметром к заимствованному Lanes.
        let lane_ptr = unsafe { NonNull::new_unchecked(self.shards.as_ptr().add(index)) };
        // SAFETY: lane_ptr инициализирован в new() (Lane записан через ptr::write).
        let claim = unsafe {
            lane_ptr.as_ref().claimed.compare_exchange(
                false,
                true,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
        };
        if claim.is_ok() {
            Some(LaneProducer {
                lane_ptr,
                write_index: 0,
                _guard: PhantomData,
            })
        } else {
            None
        }
    }

    /// Единственный жетон потребителя, опрашивающий все дорожки.
    ///
    /// Возвращает `None`, если потребитель уже выдан: второй consumer читал бы
    /// дорожки без права на `head`.
    pub fn consumer(&self) -> Option<LanesConsumer<'_>> {
        if self
            .consumer_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            Some(LanesConsumer {
                shards: self.shards,
                lane_count: self.lane_count,
                cursor: 0,
                _guard: PhantomData,
            })
        } else {
            None
        }
    }

    #[inline]
    #[must_use]
    pub fn lane_count(&self) -> usize {
        self.lane_count
    }

    #[inline]
    #[must_use]
    pub fn lane_capacity(&self) -> usize {
        self.lane_cap
    }
}

impl Drop for Lanes {
    fn drop(&mut self) {
        // SAFETY: каждая дорожка владела своим массивом слотов; освобождаем их,
        // затем массив дорожек. drop glue ни у Lane, ни у LaneSlot отсутствует.
        unsafe {
            let slot_layout = Layout::array::<LaneSlot>(self.lane_cap).expect("layout overflow");
            let lane_ptr = self.shards.as_ptr();
            for i in 0..self.lane_count {
                let slots = lane_ptr.add(i).read().slots;
                dealloc(slots.as_ptr().cast::<u8>(), slot_layout);
            }
            let lane_layout = Layout::array::<Lane>(self.lane_count).expect("layout overflow");
            dealloc(lane_ptr.cast::<u8>(), lane_layout);
        }
    }
}

// SAFETY: Lanes — массив дорожек, каждая дорожка синхронизируется двумя
// атомарными индексами на отдельных кэш-линиях. NonNull<Lane> — !Send и !Sync
// в std, поэтому нужны оба impl: Send (дорожка закреплена за одним потоком,
// consumer забирается в один поток) и Sync (UnsafeCell в LaneSlot).
unsafe impl Sync for Lanes {}
unsafe impl Send for Lanes {}

/// Монопольный жетон производителя одной дорожки.
///
/// Ровно один поток пишет в дорожку, поэтому `tail` обновляется обычным
/// release-store, без CAS. Жетон закреплён за потоком, не копируется и не
/// клонируется, и пережить `Lanes` не может (жизненный параметр).
pub struct LaneProducer<'r> {
    lane_ptr: NonNull<Lane>,
    write_index: usize,
    _guard: PhantomData<&'r Lanes>,
}

// SAFETY: жетон переносится в поток вместе с монопольным правом записи на
// дорожку; сам указатель внутри `Lanes` живёт ровно столько, сколько жетон.
// NonNull — !Send в std, поэтому impl нужен явно.
unsafe impl Send for LaneProducer<'_> {}

impl LaneProducer<'_> {
    #[inline]
    fn lane(&self) -> &Lane {
        // SAFETY: Lane инициализирована, жетон не переживает Lanes.
        unsafe { &*self.lane_ptr.as_ptr() }
    }

    /// Есть ли место в дорожке: Acquire-load `head`, чтобы увидеть освобождённые
    /// потребителем слоты.
    #[inline]
    #[must_use]
    pub fn ready(&self) -> bool {
        let lane = self.lane();
        self.write_index - lane.head.load() < lane.cap
    }

    /// Опубликовать строку/байты в свою дорожку.
    ///
    /// `Some(monotонный номер публикации)` (1, 2, 3, ...) при успехе; `None`,
    /// если дорожка забита (потребитель не трогает `head`) либо payload длиннее
    /// [`LANE_SLOT_PAYLOAD`]. Блокировки нет, CAS нет.
    #[inline]
    pub fn publish(&mut self, text: &[u8]) -> Option<usize> {
        if text.len() > LANE_SLOT_PAYLOAD {
            return None;
        }
        let lane = self.lane();
        let tail = self.write_index;
        // Acquire-load head: потребитель отпустил слоты до head, значит
        // дистанция tail - head не превысит cap.
        if tail - lane.head.load() >= lane.cap {
            return None;
        }
        let slot = unsafe { &*lane.slots.as_ptr().add(tail & lane.mask) };
        slot.fill(text);
        // Release: payload дорожки опубликован под индексом tail.
        lane.tail.store(tail + 1);
        self.write_index = tail + 1;
        Some(tail + 1)
    }

    /// То же, что [`publish`](Self::publish), но для `&str`.
    #[inline]
    pub fn publish_str(&mut self, s: &str) -> Option<usize> {
        self.publish(s.as_bytes())
    }

    /// Сколько элементов опубликовано в эту дорожку.
    #[inline]
    #[must_use]
    pub fn written(&self) -> usize {
        self.write_index
    }
}

/// Guard дорожки: `head` публикуется и при нормальном завершении прохода по
/// дорожке, и при панике в callback. Иначе недочитанные элементы были бы
/// выданы повторно в следующем `drain`, а производитель упрётся в забитую
/// дорожку намертво.
struct LaneReleaseGuard<'l> {
    lane: &'l Lane,
    release: usize,
}

impl Drop for LaneReleaseGuard<'_> {
    fn drop(&mut self) {
        self.lane.head.store(self.release);
    }
}

/// Единственный жетон потребителя: циклически опрашивает дорожки по кругу.
pub struct LanesConsumer<'r> {
    shards: NonNull<Lane>,
    lane_count: usize,
    /// Дорожка, с которой начинается следующий `drain` — round-robin.
    cursor: usize,
    _guard: PhantomData<&'r Lanes>,
}

// SAFETY: consumer забирается в единственный поток-потребитель; массив дорожек
// живёт ровно столько, сколько жетон. NonNull — !Send в std, impl нужен явно.
unsafe impl Send for LanesConsumer<'_> {}

impl LanesConsumer<'_> {
    /// Есть ли данные хотя бы в одной дорожке.
    #[inline]
    #[must_use]
    pub fn ready(&self) -> bool {
        unsafe {
            let base = self.shards.as_ptr();
            for k in 0..self.lane_count {
                let lane = &*base.add(k);
                if lane.tail.load() != lane.head.load() {
                    return true;
                }
            }
        }
        false
    }

    /// Вычерпать данные всех дорожек по очереди, отдавая каждый payload в
    /// `on_chunk`. Возвращает общее число вычерпанных элементов.
    ///
    /// Порядок: дорожки идут циклически начиная с `cursor`, внутри дорожки —
    /// строгий FIFO. После передачи payload в callback потребитель обновляет
    /// `head` release-store-ом, и дорожка снова открыта её производителю.
    ///
    /// `cursor` сдвигается на одну дорожку после каждого `drain`, поэтому
    /// следующий `drain` начинается со следующей дорожки: горячая дорожка не
    /// получает приоритет намертво. Освобождение дорожки делает guard, поэтому
    /// паника в `on_chunk` не клинит очередь и не порождает дублей; теряются
    /// только элементы этой дорожки, которые не успели быть переданы.
    pub fn drain(&mut self, mut on_chunk: impl FnMut(&[u8])) -> usize {
        let mut total = 0;
        for step in 0..self.lane_count {
            let k = (self.cursor + step) % self.lane_count;
            let lane = unsafe { &*self.shards.as_ptr().add(k) };
            let head = lane.head.load();
            let tail = lane.tail.load();
            if tail != head {
                let slots = lane.slots.as_ptr();
                // Release: все позиции до tail прочитаны, producer может писать.
                let _guard = LaneReleaseGuard {
                    lane,
                    release: tail,
                };
                for i in head..tail {
                    let slot = unsafe { &*slots.add(i & lane.mask) };
                    on_chunk(slot.payload());
                }
                total += tail - head;
            }
        }
        self.cursor = (self.cursor + 1) % self.lane_count;
        total
    }
}

// ---------------------------------------------------------------------------
// Тесты
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Геометрия: каждый слот и каждый счётчик — ровно одна кэш-линия.
    #[test]
    fn cache_line_geometry() {
        assert_eq!(core::mem::size_of::<SeqSlot>(), CACHE_LINE);
        assert_eq!(core::mem::align_of::<SeqSlot>(), CACHE_LINE);
        assert_eq!(core::mem::size_of::<LaneSlot>(), CACHE_LINE);
        assert_eq!(core::mem::align_of::<LaneSlot>(), CACHE_LINE);
        assert_eq!(core::mem::size_of::<Padded>(), CACHE_LINE);
        assert_eq!(core::mem::align_of::<Padded>(), CACHE_LINE);
        assert_eq!(core::mem::align_of::<Lane>(), CACHE_LINE);
        // tail + head + (slots/cap/mask/claimed) = ровно 3 кэш-линии.
        assert_eq!(core::mem::size_of::<Lane>(), 3 * CACHE_LINE);
    }

    /// seq-per-slot: полный круг, обратное давление, второй раунд в тех же слотах.
    #[test]
    fn seq_ring_roundtrip_and_backpressure() {
        let ring = SeqRing::new(4);
        let mut p = ring.producer().expect("первый производитель");
        let mut c = ring.consumer().expect("первый потребитель");

        for i in 0..4 {
            assert_eq!(p.publish_str(&format!("term{i}")), Some(i + 1));
        }
        // Кольцо забито: потребитель не отпустил слоты — производитель уходит
        // back-lock-free, ничего не блокируя.
        assert!(!p.ready());
        assert_eq!(p.publish(b"overflow"), None);
        assert_eq!(p.written(), 4);

        let mut out: Vec<Vec<u8>> = Vec::new();
        assert_eq!(c.drain(|chunk| out.push(chunk.to_vec())), 4);
        let expected: Vec<Vec<u8>> = (0..4).map(|i| format!("term{i}").into_bytes()).collect();
        assert_eq!(out, expected);

        // Слоты освобождены маркерами — производитель снова готов писать.
        assert!(p.ready());
        assert_eq!(p.publish_str("second-round"), Some(5));
        assert_eq!(c.drain(|_| {}), 1);
        assert_eq!(c.consumed(), 5);
    }

    /// Payload длиннее слота — publish отказывает, состояние не меняется.
    #[test]
    fn seq_ring_rejects_oversized_payload() {
        let ring = SeqRing::new(4);
        let mut p = ring.producer().expect("первый производитель");
        assert_eq!(p.publish(&[b'a'; SEQ_SLOT_PAYLOAD + 1]), None);
        let long = "a".repeat(SEQ_SLOT_PAYLOAD + 1);
        assert_eq!(p.publish_str(&long), None);
        assert_eq!(p.written(), 0);
        assert!(p.ready());
    }

    /// seq-per-slot под двумя потоками: FIFO-порядок доходит до потребителя
    /// ровно в том же порядке, что записал производитель.
    #[test]
    fn seq_ring_two_threads_fifo() {
        const ITEMS: usize = 4000;
        let ring = Arc::new(SeqRing::new(16));

        let producer_ring = Arc::clone(&ring);
        let producer = std::thread::spawn(move || {
            let mut p = producer_ring.producer().expect("производитель кольца");
            let mut sent = 0;
            for k in 0..ITEMS {
                loop {
                    if p.publish_str(&format!("chunk-{k}")).is_some() {
                        sent += 1;
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
            sent
        });

        let consumer_ring = Arc::clone(&ring);
        let consumer = std::thread::spawn(move || {
            let mut c = consumer_ring.consumer().expect("потребитель кольца");
            let mut seen: Vec<Vec<u8>> = Vec::new();
            while seen.len() < ITEMS {
                c.drain(|chunk| seen.push(chunk.to_vec()));
                std::hint::spin_loop();
            }
            seen
        });

        assert_eq!(producer.join().unwrap(), ITEMS);
        let seen = consumer.join().unwrap();
        let expected: Vec<Vec<u8>> = (0..ITEMS)
            .map(|k| format!("chunk-{k}").into_bytes())
            .collect();
        assert_eq!(seen, expected);
    }

    /// Одна дорожка = чистый SPSC: монопольный producer, обратное давление по head.
    #[test]
    fn lane_is_exclusive_and_backpressured() {
        let lanes = Lanes::new(2, 4);
        let mut p = lanes.lane(0).expect("дорожка 0");

        for i in 0..4 {
            assert!(p.publish_str(&format!("m{i}")).is_some());
        }
        assert!(!p.ready());
        assert_eq!(p.publish(b"overflow"), None);

        let mut c = lanes.consumer().expect("потребитель дорожек");
        assert_eq!(c.drain(|_| {}), 4);
        // Потребитель обновил head — дорожка снова открыта её производителю.
        assert!(p.ready());
        assert!(p.publish_str("after-drain").is_some());
        assert_eq!(c.drain(|_| {}), 1);
    }

    /// Единственный consumer опрашивает дорожки циклически: внутри дорожки —
    /// строгий FIFO, а между `drain` курсор двигается на следующую дорожку.
    #[test]
    fn consumer_polls_lanes_round_robin() {
        let lanes = Lanes::new(3, 8);
        let mut p0 = lanes.lane(0).expect("дорожка 0");
        let mut p1 = lanes.lane(1).expect("дорожка 1");
        let mut p2 = lanes.lane(2).expect("дорожка 2");
        for i in 0..3 {
            assert!(p0.publish_str(&format!("a{i}")).is_some());
            assert!(p1.publish_str(&format!("b{i}")).is_some());
            assert!(p2.publish_str(&format!("c{i}")).is_some());
        }

        let mut c = lanes.consumer().expect("потребитель дорожек");

        let mut first: Vec<String> = Vec::new();
        assert_eq!(
            c.drain(|chunk| first.push(String::from_utf8_lossy(chunk).into_owned())),
            9
        );
        assert_eq!(
            first,
            ["a0", "a1", "a2", "b0", "b1", "b2", "c0", "c1", "c2"]
        );
        // Все дорожки вычерпаны — следующая волна пуста, курсор уже сдвинут.
        assert!(!c.ready());
        assert_eq!(c.drain(|_| {}), 0);

        // Вторая волна: курсор = 1, дорожки 1 и 2 пусты, значит дорожка 2
        // опрашивается раньше дорожки 0.
        assert!(p2.publish_str("z").is_some());
        assert!(p0.publish_str("x").is_some());
        let mut second: Vec<String> = Vec::new();
        assert_eq!(
            c.drain(|chunk| second.push(String::from_utf8_lossy(chunk).into_owned())),
            2
        );
        assert_eq!(second, ["z", "x"]);

        // Третья волна: после пустого drain и drain второй волны курсор = 0,
        // значит дорожки снова идут 0, 1, 2.
        assert!(p2.publish_str("c3").is_some());
        assert!(p0.publish_str("a3").is_some());
        assert!(p1.publish_str("b3").is_some());
        let mut third: Vec<String> = Vec::new();
        assert_eq!(
            c.drain(|chunk| third.push(String::from_utf8_lossy(chunk).into_owned())),
            3
        );
        assert_eq!(third, ["a3", "b3", "c3"]);
    }

    /// MPSC под нагрузкой: много потоков-производителей пишут в свои дорожки,
    /// один потребитель собирает всё; потерять ничего не должно, и порядок
    /// внутри каждой дорожки обязан остаться FIFO.
    #[test]
    fn lanes_mpsc_across_threads() {
        const PRODUCERS: usize = 8;
        const PER_PRODUCER: usize = 500;
        const TOTAL: usize = PRODUCERS * PER_PRODUCER;

        let lanes = Arc::new(Lanes::new(PRODUCERS, 32));

        let producers: Vec<_> = (0..PRODUCERS)
            .map(|i| {
                let lanes = Arc::clone(&lanes);
                std::thread::spawn(move || {
                    let mut p = lanes.lane(i).expect("производитель дорожки i");
                    let mut sent: Vec<Vec<u8>> = Vec::new();
                    for k in 0..PER_PRODUCER {
                        let payload = format!("p{i}#{k}").into_bytes();
                        loop {
                            if p.publish(&payload).is_some() {
                                sent.push(payload);
                                break;
                            }
                            std::hint::spin_loop();
                        }
                    }
                    sent
                })
            })
            .collect();

        let consumer_lanes = Arc::clone(&lanes);
        let consumer = std::thread::spawn(move || {
            let mut c = consumer_lanes.consumer().expect("потребитель дорожек");
            let mut got: Vec<Vec<u8>> = Vec::new();
            while got.len() < TOTAL {
                if c.drain(|chunk| got.push(chunk.to_vec())) == 0 {
                    std::hint::spin_loop();
                }
            }
            got
        });

        let mut expected: Vec<Vec<u8>> = Vec::new();
        for handle in producers {
            expected.extend(handle.join().unwrap());
        }
        let got = consumer.join().unwrap();
        assert_eq!(got.len(), TOTAL);

        // Внутри каждой дорожки строгий FIFO: p{i}#k доходит по возрастанию k,
        // именно этот порядок спина producers не должна ломать.
        let mut per_lane: Vec<Vec<usize>> = vec![Vec::new(); PRODUCERS];
        for chunk in &got {
            let s = String::from_utf8_lossy(chunk).into_owned();
            let (producer, k) = s.split_once('#').expect("формат p{i}#{k}");
            let lane = producer[1..].parse::<usize>().expect("номер производителя");
            per_lane[lane].push(k.parse::<usize>().expect("номер элемента"));
        }
        for (i, seen) in per_lane.iter().enumerate() {
            assert_eq!(
                *seen,
                (0..PER_PRODUCER).collect::<Vec<usize>>(),
                "дорожка {i}: FIFO-порядок сломан"
            );
        }

        // Ничего не потеряно: мультисеты совпадают.
        let mut got_sorted = got;
        expected.sort();
        got_sorted.sort();
        assert_eq!(got_sorted, expected);
    }

    /// Массив дорожек выровнен по кэш-линиям: head/tail каждой дорожки
    /// занимает собственную линию, без наложений.
    #[test]
    fn lane_counters_are_line_isolated() {
        let lanes = Lanes::new(4, 8);
        let base = lanes.shards.as_ptr() as usize;
        assert_eq!(base % CACHE_LINE, 0);
        let stride = core::mem::size_of::<Lane>();
        for i in 0..4 {
            let lane = unsafe { &*lanes.shards.as_ptr().add(i) };
            let tail_addr = (&raw const lane.tail) as usize;
            let head_addr = (&raw const lane.head) as usize;
            assert_eq!(tail_addr % CACHE_LINE, 0);
            assert_eq!(head_addr % CACHE_LINE, 0);
            assert_eq!(head_addr - tail_addr, CACHE_LINE);
            assert_eq!((base + i * stride) % CACHE_LINE, 0);
        }
    }

    /// Жетоны монопольны: второй производитель/потребитель не выдаётся —
    /// именно это держит lock-free схему от «двух приватных индексов в один слот».
    #[test]
    fn tokens_are_exclusive() {
        let ring = SeqRing::new(4);
        assert!(ring.producer().is_some());
        assert!(ring.producer().is_none(), "второй производитель запрещён");
        assert!(ring.consumer().is_some());
        assert!(ring.consumer().is_none(), "второй потребитель запрещён");

        let lanes = Lanes::new(2, 4);
        assert!(lanes.lane(0).is_some());
        assert!(lanes.lane(0).is_none(), "дорожка 0 уже закреплена");
        assert!(lanes.lane(1).is_some(), "дорожка 1 свободна");
        assert!(lanes.consumer().is_some());
        assert!(
            lanes.consumer().is_none(),
            "потребитель один на все дорожки"
        );
    }

    /// Паника в callback `SeqConsumer::drain` отпускает слот через guard:
    /// маркер не остаётся «занятым», кольцо не клинит.
    #[test]
    fn panicking_callback_releases_ring() {
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {})); // паника из callback — не шум
        let ring = SeqRing::new(4);
        let mut p = ring.producer().expect("производитель");
        let mut c = ring.consumer().expect("потребитель");
        for i in 0..4 {
            assert!(p.publish_str(&format!("t{i}")).is_some());
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            c.drain(|chunk| {
                // На t1 callback паникует — проверка эквивалентна if + panic!.
                assert_ne!(chunk, b"t1", "callback паникует на t1");
            })
        }));
        std::panic::set_hook(hook);
        assert!(result.is_err());
        // Слоты 0 и 1 отпущены: write_index = 4, слот 4 & 3 = 0 с маркером 4,
        // значит производитель снова готов писать в кольцо.
        assert!(p.ready());
        assert!(p.publish_str("after-panic").is_some());
        // Раунд t1 пропущен — кольцо продолжает работу: t2, t3 и after-panic
        // доходят до потребителя.
        assert_eq!(c.drain(|_| {}), 3);
        assert_eq!(c.consumed(), 5);
    }

    /// Паника в callback `LanesConsumer::drain` публикует head через guard:
    /// дорожка не клинит, и вычерпанные элементы не выдаются повторно.
    #[test]
    fn panicking_callback_releases_lane() {
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let lanes = Lanes::new(2, 4);
        let mut p = lanes.lane(0).expect("дорожка 0");
        let mut c = lanes.consumer().expect("потребитель");
        for i in 0..4 {
            assert!(p.publish_str(&format!("m{i}")).is_some());
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            c.drain(|chunk| {
                assert_ne!(chunk, b"m2", "callback паникует на m2");
            })
        }));
        std::panic::set_hook(hook);
        assert!(result.is_err());
        // Дорожка отпущена целиком (head = tail = 4): производитель пишет снова.
        assert!(p.ready());
        assert!(p.publish_str("next").is_some());
        let mut out: Vec<String> = Vec::new();
        c.drain(|chunk| out.push(String::from_utf8_lossy(chunk).into_owned()));
        assert_eq!(out, ["next"]);
    }
}
