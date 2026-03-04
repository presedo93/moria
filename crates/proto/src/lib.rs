pub mod market_data {
    tonic::include_proto!("market_data");
}

pub mod strategy {
    tonic::include_proto!("strategy");
}

pub mod order {
    tonic::include_proto!("order");
}

pub mod risk {
    tonic::include_proto!("risk");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("moria_descriptor");
