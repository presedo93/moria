fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("moria_descriptor.bin"))
        .compile_protos(
            &[
                "../../proto/market_data.proto",
                "../../proto/strategy.proto",
                "../../proto/order.proto",
                "../../proto/risk.proto",
            ],
            &["../../proto"],
        )?;
    Ok(())
}
