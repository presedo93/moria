use tonic::{Request, Status};

const INTERNAL_TOKEN_HEADER: &str = "x-internal-token";

pub fn authorize_request<T>(request: &Request<T>, required_token: Option<&str>) -> Result<(), Status> {
    let Some(expected) = required_token else {
        return Ok(());
    };

    let provided = request
        .metadata()
        .get(INTERNAL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if provided == expected {
        Ok(())
    } else {
        Err(Status::unauthenticated("missing or invalid internal service token"))
    }
}

pub fn attach_internal_token<T>(request: &mut Request<T>, token: Option<&str>) -> Result<(), Status> {
    let Some(token) = token else {
        return Ok(());
    };

    let value = tonic::metadata::MetadataValue::try_from(token)
        .map_err(|_| Status::internal("invalid internal token format"))?;
    request.metadata_mut().insert(INTERNAL_TOKEN_HEADER, value);
    Ok(())
}
