fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
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
