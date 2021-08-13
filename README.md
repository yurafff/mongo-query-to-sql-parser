# MongoDB to SQL translator for Humans
Library that translates MongoDB find queries 
into valid SQL queries

## Installation Guide
```shell
npm install '<path-to-library>'
```
## Run tests
To run tests execute this command in shell
```shell
npm test
```
## Use from shell
Example usage from shell:
```shell
cd mongo-query-to-sql-parser
npm install
ts-node lib/cli.ts --query 'db.advertisement.find({ $or: [ { likes: { $lt: 30 } }, { userScore: 55 } ]})'
```
## Use from code
```typescript
import { convert } from "mongo-query-to-sql-parser";
const mongoQuery = "<mongo query goes here>";
convert(mongoQuery);
```

## Supported operators
* $or
* $and
* $lt
* $lte
* $gt
* $gte
* $ne
* $in
