Tekopia is a Go program that performs an analysis of Oracle database structure changes and their impact on customizations during a PeopleSoft upgrade.

The program finds records and fields renamed in the new software release, records now views (and vice versa), field length changes, and obsolete records and fields. It searches custom SQRs, Queries, SQL and PeopleCode for references to such objects. The report provides a detail impact analysis, as well as a summary of the total number of custom SQR, PeopleCode, SQL and Query objects impacted by the various changes in the new software release.

Tekopia requires an old release demo- and a new release upgraded database on the same PeopleTools version, as well as a database link from the newly upgraded- to the old version demo database.

Prior to running the program in the newly upgraded database, run an Application Designer database compare in the upgraded database with the old release demo database as the target. Run 'Compare Records' only. Deselect all report filters, select 'Update Project Item Status and Child Definitions', Compare by Release (select the application version of the old version demo) and set the target orientation to 'PeopleSoft Vanilla'.

The two Application Designer projects referenced in the variables upgrade and upgcust should exist in the newly upgraded database prior to running the program.

Tekopia runs in one of four modes - report changes, report changes and analyze SQRs, report changes and analyze online objects, report and analyze impact on SQRs and online objects.

The program references and uses the go-oci8 oracle driver which can be found on https://github.com/mattn/go-oci8. Please review the applicable license agreement.
