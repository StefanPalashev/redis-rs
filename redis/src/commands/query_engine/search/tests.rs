mod search_tests {
    use crate::geo::Unit;
    use crate::search::*;
    use rstest::rstest;
    use std::ops::Bound;

    const PRODUCTS_INDEX: &str = "products_idx";
    const TITLE: &str = "title";
    const DESCRIPTION: &str = "description";
    const PRICE: &str = "price";
    const COST: &str = "cost";
    const RATING: &str = "rating";
    const HELLO_WORLD: &str = "hello world";

    #[test]
    fn test_simple_search() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*");
        assert_eq!(cmd.into_args(), "FT.SEARCH products_idx *");
    }

    #[test]
    fn test_search_with_query() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "@title:laptop");
        assert_eq!(cmd.into_args(), "FT.SEARCH products_idx @title:laptop");
    }

    #[test]
    fn test_search_with_query_containing_spaces() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, HELLO_WORLD);
        assert_eq!(cmd.into_args(), "FT.SEARCH products_idx \"hello world\"");
    }

    #[test]
    fn test_search_with_nocontent() {
        let cmd =
            FtSearchCommand::new(PRODUCTS_INDEX, "*").options(SearchOptions::new().nocontent());
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * NOCONTENT DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_verbatim() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, HELLO_WORLD)
            .options(SearchOptions::new().verbatim());
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx \"hello world\" VERBATIM DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_withscores() {
        let cmd =
            FtSearchCommand::new(PRODUCTS_INDEX, "*").options(SearchOptions::new().withscores());
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * WITHSCORES DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_withsortkeys() {
        let cmd =
            FtSearchCommand::new(PRODUCTS_INDEX, "*").options(SearchOptions::new().withsortkeys());
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * WITHSORTKEYS DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_numeric_filter() {
        // Test with no bounds
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*").options(SearchOptions::new().filter(
            NumericFilter::new(PRICE, Bound::Unbounded, Bound::Unbounded),
        ));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * FILTER price -inf +inf DIALECT 2"
        );

        // Test with included bounds
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*").options(SearchOptions::new().filter(
            NumericFilter::new(PRICE, Bound::Included(100.0), Bound::Included(500.0)),
        ));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * FILTER price 100.0 500.0 DIALECT 2"
        );

        // Test with excluded bounds
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*").options(SearchOptions::new().filter(
            NumericFilter::new(PRICE, Bound::Excluded(100.0), Bound::Excluded(500.0)),
        ));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * FILTER price (100.0 (500.0 DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_multiple_numeric_filters() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*").options(
            SearchOptions::new()
                .filter(NumericFilter::new(
                    PRICE,
                    Bound::Included(100.0),
                    Bound::Included(500.0),
                ))
                .filter(NumericFilter::new(
                    RATING,
                    Bound::Included(4.0),
                    Bound::Included(5.0),
                )),
        );
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * FILTER price 100.0 500.0 FILTER rating 4.0 5.0 DIALECT 2"
        );

        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*").options(
            SearchOptions::new()
                .filter(NumericFilter::new(
                    PRICE,
                    Bound::Included(100.0),
                    Bound::Excluded(500.0),
                ))
                .filter(NumericFilter::new(
                    PRICE,
                    Bound::Excluded(600.0),
                    Bound::Included(1000.0),
                ))
                .filter(NumericFilter::new(
                    RATING,
                    Bound::Included(4.0),
                    Bound::Included(5.0),
                )),
        );
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * FILTER price 100.0 (500.0 FILTER price (600.0 1000.0 FILTER rating 4.0 5.0 DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_geofilter() {
        let cmd =
            FtSearchCommand::new(PRODUCTS_INDEX, "*").options(SearchOptions::new().geofilter(
                GeoFilter::new("location", -122.41, 37.77, 5.0, Unit::Kilometers),
            ));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * GEOFILTER location -122.41 37.77 5.0 km DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_inkeys() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*")
            .options(SearchOptions::new().inkey("product:1").inkey("product:2"));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * INKEYS 2 product:1 product:2 DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_inkeys_bulk() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*").options(SearchOptions::new().inkeys([
            "product:1",
            "product:2",
            "product:3",
        ]));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * INKEYS 3 product:1 product:2 product:3 DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_infields() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*")
            .options(SearchOptions::new().infield(TITLE).infield(DESCRIPTION));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * INFIELDS 2 title description DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_infields_bulk() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*")
            .options(SearchOptions::new().infields([TITLE, DESCRIPTION, PRICE]));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * INFIELDS 3 title description price DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_return_fields() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*").options(
            SearchOptions::new()
                .return_field(ReturnField::new(TITLE))
                .return_field(ReturnField::new(PRICE)),
        );
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * RETURN 2 title price DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_return_field_alias() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*")
            .options(SearchOptions::new().return_field(ReturnField::new(PRICE).alias(COST)));
        // "price AS cost" is 3 arguments
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * RETURN 3 price AS cost DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_return_fields_bulk() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*").options(
            SearchOptions::new().return_fields([
                ReturnField::new(TITLE),
                ReturnField::new(DESCRIPTION),
                ReturnField::new(PRICE),
            ]),
        );
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * RETURN 3 title description price DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_return_fields_bulk_with_alias() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*").options(
            SearchOptions::new()
                .return_fields([ReturnField::new(TITLE), ReturnField::new(PRICE).alias(COST)]),
        );
        // "title" is 1 arg, "price AS cost" is 3 args = 4 total
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * RETURN 4 title price AS cost DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_summarize() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*").options(
            SearchOptions::new().summarize(
                SummarizeOptions::new()
                    .field(TITLE)
                    .frags(3)
                    .len(50)
                    .separator("..."),
            ),
        );
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * SUMMARIZE FIELDS 1 title FRAGS 3 LEN 50 SEPARATOR ... DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_summarize_multiple_fields() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*").options(
            SearchOptions::new().summarize(
                SummarizeOptions::new()
                    .fields([TITLE, PRICE, DESCRIPTION])
                    .frags(3)
                    .len(50)
                    .separator("..."),
            ),
        );
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * SUMMARIZE FIELDS 3 title price description FRAGS 3 LEN 50 SEPARATOR ... DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_highlight() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "premium").options(
            SearchOptions::new().highlight(
                HighlightOptions::new()
                    .field(DESCRIPTION)
                    .tags("<b>", "</b>"),
            ),
        );
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx premium HIGHLIGHT FIELDS 1 description TAGS <b> </b> DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_slop() {
        let cmd =
            FtSearchCommand::new(PRODUCTS_INDEX, HELLO_WORLD).options(SearchOptions::new().slop(2));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx \"hello world\" SLOP 2 DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_inorder() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, HELLO_WORLD)
            .options(SearchOptions::new().inorder());
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx \"hello world\" INORDER DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_language() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*")
            .options(SearchOptions::new().language(SearchLanguage::Spanish));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * LANGUAGE SPANISH DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_custom_expander() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*")
            .options(SearchOptions::new().expander("my_expander"));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * EXPANDER my_expander DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_custom_scorer() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*")
            .options(SearchOptions::new().scorer("my_scorer"));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * SCORER my_scorer DIALECT 2"
        );
    }

    #[rstest]
    #[case(ScoringFunction::Tfidf, "TFIDF")]
    #[case(ScoringFunction::TfidfDocnorm, "TFIDF.DOCNORM")]
    #[case(ScoringFunction::Bm25Std, "BM25STD")]
    #[case(ScoringFunction::Bm25StdNorm, "BM25STD.NORM")]
    #[case(ScoringFunction::Bm25StdTanh { factor: None }, "BM25STD.TANH")]
    #[case(ScoringFunction::Dismax, "DISMAX")]
    #[case(ScoringFunction::Docscore, "DOCSCORE")]
    #[case(ScoringFunction::Hamming, "HAMMING")]
    fn test_search_with_scoring_function(
        #[case] scoring_function: ScoringFunction,
        #[case] expected_scorer: &str,
    ) {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*")
            .options(SearchOptions::new().scoring_function(scoring_function));
        assert_eq!(
            cmd.into_args(),
            format!(
                "FT.SEARCH products_idx * SCORER {} DIALECT 2",
                expected_scorer
            )
        );
    }

    #[test]
    fn test_search_with_scoring_function_bm25std_tanh_custom_factor() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*").options(
            SearchOptions::new()
                .scoring_function(ScoringFunction::Bm25StdTanh { factor: Some(12) }),
        );
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * SCORER BM25STD.TANH BM25STD_TANH_FACTOR 12 DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_explainscore() {
        let cmd =
            FtSearchCommand::new(PRODUCTS_INDEX, "*").options(SearchOptions::new().explainscore());
        // Verify that WITHSCORES is automatically added, since it is mandatory for EXPLAINSCORE
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * WITHSCORES EXPLAINSCORE DIALECT 2"
        );
    }

    #[test]
    fn tests_search_with_payload() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*")
            .options(SearchOptions::new().payload("my_payload"));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * PAYLOAD my_payload DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_sortby_asc() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*")
            .options(SearchOptions::new().sortby(PRICE, Some(SortDirection::Asc)));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * SORTBY price ASC DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_sortby_desc() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*")
            .options(SearchOptions::new().sortby(PRICE, Some(SortDirection::Desc)));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * SORTBY price DESC DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_sortby_no_order() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*")
            .options(SearchOptions::new().sortby(PRICE, None));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * SORTBY price DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_limit() {
        let cmd =
            FtSearchCommand::new(PRODUCTS_INDEX, "*").options(SearchOptions::new().limit((10, 20)));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * LIMIT 10 20 DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_timeout() {
        let cmd =
            FtSearchCommand::new(PRODUCTS_INDEX, "*").options(SearchOptions::new().timeout(5000));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx * TIMEOUT 5000 DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_params() {
        const TERM: &str = "term";
        const LAPTOP: &str = "laptop";
        // Using QueryParam::new
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "@title:$term")
            .options(SearchOptions::new().param(QueryParam::new(TERM, LAPTOP)));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx @title:$term PARAMS 2 term laptop DIALECT 2"
        );

        // Using a tuple
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "@title:$term")
            .options(SearchOptions::new().param((TERM, LAPTOP)));
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx @title:$term PARAMS 2 term laptop DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_multiple_params() {
        const TERM: &str = "term";
        const LAPTOP: &str = "laptop";
        const MIN: &str = "min";
        const MAX: &str = "max";
        // Using QueryParam::new
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "@title:$term @price:[$min $max]").options(
            SearchOptions::new()
                .param(QueryParam::new(TERM, LAPTOP))
                .param(QueryParam::new(MIN, "100"))
                .param(QueryParam::new(MAX, "500")),
        );
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx \"@title:$term @price:[$min $max]\" PARAMS 6 term laptop min 100 max 500 DIALECT 2"
        );

        // Using params method
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "@title:$term @price:[$min $max]").options(
            SearchOptions::new().params([
                QueryParam::new(TERM, LAPTOP),
                QueryParam::new(MIN, "100"),
                QueryParam::new(MAX, "500"),
            ]),
        );
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx \"@title:$term @price:[$min $max]\" PARAMS 6 term laptop min 100 max 500 DIALECT 2"
        );
    }

    #[test]
    fn test_search_with_params_using_tuples() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "@title:$term @price:[$min $max]").options(
            SearchOptions::new().params([("term", "laptop"), ("min", "100"), ("max", "500")]),
        );
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx \"@title:$term @price:[$min $max]\" PARAMS 6 term laptop min 100 max 500 DIALECT 2"
        );
    }

    #[rstest]
    #[case(QueryDialect::One, "1")]
    #[case(QueryDialect::Two, "2")]
    #[case(QueryDialect::Three, "3")]
    #[case(QueryDialect::Four, "4")]
    #[allow(deprecated)]
    fn test_search_with_dialect(#[case] dialect: QueryDialect, #[case] expected_dialect: &str) {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "*")
            .options(SearchOptions::new().dialect(dialect));
        assert_eq!(
            cmd.into_args(),
            format!("FT.SEARCH products_idx * DIALECT {}", expected_dialect)
        );
    }

    #[test]
    #[allow(deprecated)]
    fn test_search_with_combined_options() {
        let cmd = FtSearchCommand::new(PRODUCTS_INDEX, "@title:laptop").options(
            SearchOptions::new()
                .filter(NumericFilter::new(
                    PRICE,
                    Bound::Included(100.0),
                    Bound::Included(500.0),
                ))
                .withscores()
                .limit((0, 10))
                .sortby(PRICE, Some(SortDirection::Asc))
                .dialect(QueryDialect::Three),
        );
        assert_eq!(
            cmd.into_args(),
            "FT.SEARCH products_idx @title:laptop WITHSCORES FILTER price 100.0 500.0 SORTBY price ASC LIMIT 0 10 DIALECT 3"
        );
    }
}
