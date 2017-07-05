# WebTableStitching

Tables on web pages ("web tables") cover a diversity of topics and can be a source of information for different tasks such as knowledge base augmentation or the ad-hoc extension of datasets. However, to use this information, the tables must first be integrated, either with each other or into existing data sources. The challenges that matching methods for this purpose have to overcome are the high heterogeneity and the small size of the tables.

To counter these problems, web tables from the same web site can be stitched before running any of the existing matching systems. This means that web tables are combined based on a schema mapping, which results in fewer and larger stitched tables.
 
This project contains the code for all methods used in "Stitching Web Tables for Improving Matching Quality" [1]. The version of the code that was used to run the experiments can be found in the "original_version" branch.
 
## How to run

The complete web table stitching process consists of three steps:

1. create union tables `scripts/create_union_tables`
2. deduplicate & discover functional dependencies `scripts/discover_functional_dependencies`
3. create stitched union tables `scripts/create_stitched_union_tables`

To match the resulting stitched union tables to a knowledge base, see the [T2K Match](https://github.com/olehmberg/T2KMatch) Project.

## Acknowledgements

This project was developed at the [Data and Web Science Group](http://dws.informatik.uni-mannheim.de/) at the [University of Mannheim](http://www.uni-mannheim.de/) using the [WInte.r Framework](https://github.com/olehmberg/winter).

## License

The Web Table Stitching code can be used under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0).

## References

[1] Lehmberg, Oliver and Bizer, Christian. "Stitching Web Tables for Improving Matching Quality" Proceedings of the VLDB Endowment - Preprint (2017).