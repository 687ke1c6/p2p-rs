fn main() {
    // `ui::EmbeddedPage` embeds `ui/dist/` in a release build. Cargo tracks
    // the files it embeds, but not files `just ui` adds, nor the dir appearing
    // after a build without it.
    println!("cargo:rerun-if-changed=../../ui/dist");
}
