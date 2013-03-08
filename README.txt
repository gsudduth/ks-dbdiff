This is a utility which will compare a "before" database with an "after" database and output SQLs to bring "before" in sync with "after".

Currently, only data that has been added is supported.

Rows that have been deleted and rows that have been updated will simply be ignored.
Those operations shouldn't be too difficult to support, but I didn't have time to put them in.
Also, this tool might fit into the Kuali portfolio better as a maven plugin. Didn't have time for that either.

Assumptions:
* "before" and "after" databases are identical from a schema perspective.

Recipe:
* Create the "before" action using the usual impex process. Something like ...
    mvn initialize -Pdb,oracle -Doracle.dba.username=SYSTEM -Doracle.dba.password=passwd \
        -Doracle.dba.url="jdbc:oracle:thin:@oracle:1521:XE" -Djdbc.username=KSBUNDLEDNEW

* Create the "after" database the same way (notice the schema name changes)...
    mvn initialize -Pdb,oracle -Doracle.dba.username=SYSTEM -Doracle.dba.password=passwd \
        -Doracle.dba.url="jdbc:oracle:thin:@oracle:1521:XE" -Djdbc.username=KSBUNDLED

* Add data to "before" either via the application UI or by bootstraping a services client or whatever.

* Run the DbDiff program (possibly changing the hardcoded schema/passwd) and the SQLs will be written to ...
    target/sqls
