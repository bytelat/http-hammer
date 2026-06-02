#[derive(Debug, Default)]
pub struct CliOptions {
    pub rps: Option<usize>,
    pub file: Option<String>,
}

/*
impl CliOptions {
    fn default() -> Self {
        CliOptions { rps: None, file: None }
    }
}*/
