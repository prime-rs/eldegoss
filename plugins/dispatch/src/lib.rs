use extism_pdk::*;

#[plugin_fn]
pub fn dispatch(name: String) -> FnResult<String> {
    Ok(format!("Hi, {}!", name))
}
