#[derive(Debug, Default)]
pub struct CliOptions {
    pub rps: Option<usize>,
    pub file: Option<String>,
    pub config: Option<String>,
}

/*
impl CliOptions {
    fn default() -> Self {
        CliOptions { rps: None, file: None, config: None }
    }
}*/
