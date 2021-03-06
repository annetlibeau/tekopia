Tekopia is a Go program that performs an analysis of Oracle database structure changes and their impact on customizations during a PeopleSoft upgrade.

The program finds records and fields renamed in the new software release, records that are now views (and vice versa), field length changes, and obsolete records and fields. It searches custom SQRs, Queries, SQL and PeopleCode for references to such objects. The report provides a detail impact analysis, as well as a summary of the total number of custom SQR, PeopleCode, SQL and Query objects impacted by the various changes in the new software release.

Tekopia requires  a database link from the new release upgraded database to an old release demo database.

Prior to running the program in the newly upgraded database, insert all records and fields into an Application Designer project in the old release demo database and copy the project to file. Run a Record compare in the upgraded database against the file. Deselect all report filters, select ‘Update Project Item Status and Child Definitions’, Compare by Release (select the application version of the old release demo) and set the target orientation to ‘PeopleSoft Vanilla’.

The two Application Designer projects referenced in the variables upgrade and upgcust should exist in the newly upgraded database prior to running the program.

Tekopia runs in one of four modes - report changes, report changes and analyze SQRs, report changes and analyze online objects, report and analyze impact on SQRs and online objects.

The program references and uses the go-oci8 Oracle driver which is copyrighted by Yasuhiro Matsumoto and governed by a separate license agreement.
