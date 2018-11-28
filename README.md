In gfsh:
```
start locator
configure pdx --auto-serializable-classes=com\.example\..*
start server
create region --name=example-region --type=PARTITION
deploy --jar build/libs/java11-example-1.0-SNAPSHOT.jar
```
