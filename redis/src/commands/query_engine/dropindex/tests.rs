mod dropindex_tests {
    use crate::search::*;

    const INDEX_NAME: &str = "index";

    #[test]
    fn test_dropindex_basic() {
        let cmd = FtDropIndexCommand::new(INDEX_NAME);
        assert_eq!(cmd.into_args(), "FT.DROPINDEX index");
    }

    #[test]
    fn test_dropindex_with_delete_documents() {
        let cmd = FtDropIndexCommand::new(INDEX_NAME).delete_documents();
        assert_eq!(cmd.into_args(), "FT.DROPINDEX index DD");
    }
}
