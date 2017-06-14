# WebTableStitching

Source code of the Web Tables Stichting Project.

Use the classes in the de.uni_mannheim.informatik.dws.tnt.match.cli package to run the code.

- de.uni_mannheim.informatik.dws.tnt.match.cli.CreateUnionTables: Creates union tables of all original web tables with the same schema
- de.uni_mannheim.informatik.dws.tnt.match.cli.DiscoverFunctionalDependencies: deduplicates the union tables and discovers the functional dependencies (needed for the schema matching)
- de.uni_mannheim.informatik.dws.tnt.match.cli.CreateStitchedUnionTables: Creates stitched union tables via a schema matching of all provided union tables

To match the resulting stitched union tables to a knowledge base, see the [T2K Match](https://github.com/olehmberg/T2KMatch) Project.