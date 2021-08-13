#!/usr/bin/env node
import { Command } from 'commander';
import { convert } from './parser';

const program = new Command();

// parse query argument
program
  .version('1.0.0')
  .option('-q, --query <mongoQuery>', 'MongoDB query')
  .parse(process.argv);

// translate query into SQL
const options = program.opts();
const sqlQuery = convert(options.query);

// output
console.log(sqlQuery);
