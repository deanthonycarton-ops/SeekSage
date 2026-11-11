fn main() {
    println!("cargo:rerun-if-changed=app_icon.ico");
    #[cfg(windows)]
    {
        let mut res = winresource::WindowsResource::new();
        res.set_icon("app_icon.ico");
        res.set("ProductName", "SeekSage");
        res.set("FileDescription", "SeekSage");
        res.set("OriginalFilename", "SeekSage.exe");
        res.compile().expect("Failed to compile Windows icon resource");
    }
}
