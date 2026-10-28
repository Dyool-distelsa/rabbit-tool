fn git(args: &[&str]) -> Option<String> {
    let out = std::process::Command::new("git").args(args).output().ok()?;
    out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn main() {
    // Datos de la compilación para la ayuda ("Acerca de"). Sin git, quedan vacíos.
    println!("cargo:rustc-env=BUILD_COMMIT={}", git(&["rev-parse", "--short", "HEAD"]).unwrap_or_default());
    println!("cargo:rustc-env=BUILD_DATE={}", git(&["log", "-1", "--format=%cI"]).unwrap_or_default());
    println!("cargo:rerun-if-changed=../.git/HEAD");
    tauri_build::build()
}
