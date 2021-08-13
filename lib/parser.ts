import parse from 'ejson-shell-parser';
import * as EJSON from 'mongodb-extended-json';
import * as queryLanguage from 'mongodb-language-model';

// Declared interfaces to represent JSON type
interface JsonMap {
  [member: string]: string | number | boolean | null | JsonArray | JsonMap;
}
type JsonArray = Array<string | number | boolean | null | JsonArray | JsonMap>;

// Declared types that represents MongoDB operand
type SimpleOperandType = string | number | boolean | null;
type OperandType = SimpleOperandType | Array<SimpleOperandType>;

// Declared constants for parser
const SELECT_ALL_FIELDS = '*';

const FIND_QUERY_REGEXP = /^db.\w*.find/i;

const OPERATORS_MAP: { [key: string]: string } = {
  $or: 'OR',
  $and: 'AND',
  $lt: '<',
  $lte: '<=',
  $gt: '>',
  $gte: '>=',
  $ne: '!=',
  $in: 'IN',
};

enum LogicalOperator {
  OR = 'OR',
  AND = 'AND',
}

/**
 * Translates MongoDB operator into SQL operator
 * @param mongoOperator MongoDB operator to translate
 */
const getSQLOperator = (mongoOperator: string): string => {
  if (OPERATORS_MAP.hasOwnProperty(mongoOperator)) {
    return OPERATORS_MAP[mongoOperator];
  }
  throw new Error(`MongoDB operator: ${mongoOperator} not supported.`);
};

/**
 * Validates if it's find query and adds semicolon if necessary
 * @param query MongoDB query without db and collection names
 */
const checkAndFormatQuery = (query: string): string => {
  if (!FIND_QUERY_REGEXP.test(query)) {
    throw new Error('Only find queries supported');
  }
  if (!query.endsWith(';')) {
    query += ';';
  }
  return query;
};

/**
 * Transforms operand to sql format
 * @param operand operand we need to format
 * @return sql-compatible operand
 */
const fixOperandFormat = (operand: OperandType): string => {
  // add single quotes to string literals
  if (typeof operand === 'string') {
    return `'${operand}'`;
  }
  // consider thar boolean type is BIT data type
  if (typeof operand === 'boolean') {
    return Number(operand).toString();
  }
  // stringify array for IN operator
  if (operand instanceof Array) {
    const formattedArray = operand
      .map(item => fixOperandFormat(item))
      .join(', ');
    return `(${formattedArray})`;
  }
  return operand.toString();
};

/**
 * Joins statements under AND/OR logical operator
 * @param logicOperator logical operator for joining
 * @param parsedStatements statements to join
 */
const joinResults = (
  logicOperator: LogicalOperator,
  parsedStatements: string[],
) => {
  const parsedNested = parsedStatements.join(` ${logicOperator} `);
  return `(${parsedNested})`;
};

/**
 * Translates group of MongoDB statements
 * combined by logical operator
 * into SQL where statement
 * @param field $or/$and operator
 * @param value array of nested statements
 * @param logicalOperator top level logical operator
 */
const mapLogicalOperatorSQL = (
  field: string,
  value: JsonArray,
  logicalOperator: LogicalOperator,
): string[] => {
  // recursively parse nested statements
  const nestedLogicalOperator = getSQLOperator(field) as LogicalOperator;
  const parsedExpressions = value.map((expr: JsonMap) =>
    mapQueryToSQL(expr, nestedLogicalOperator),
  );
  // join results
  if (getSQLOperator(field) !== logicalOperator) {
    return [joinResults(nestedLogicalOperator, parsedExpressions)];
  }
  return parsedExpressions;
};

/**
 * Translates MongoDB
 * comparison statement
 * into SQL where statement
 * @param field over which comparison is made
 * @param value operator: operand map
 * @param logicOperator
 */
const mapComparisonOperatorSQL = (
  field: string,
  value: JsonMap,
  logicOperator: LogicalOperator,
): string[] => {
  const comparisonConditions = Object.keys(value).map(operator => {
    // translate comparison statement into sql
    const operand = value[operator] as OperandType;
    const formattedOperand = fixOperandFormat(operand);
    return `${field} ${getSQLOperator(operator)} ${formattedOperand}`;
  });
  // return statements without join of only one
  // statement or logical operator equal to AND
  if (
    comparisonConditions.length === 1 ||
    logicOperator === LogicalOperator.AND
  ) {
    return comparisonConditions;
  } else {
    return [joinResults(LogicalOperator.AND, comparisonConditions)];
  }
};

/**
 * Translate $eq statement into sql format
 * @param field over which matching is made
 * @param value operand value
 */
const mapEqualityOperatorToSQL = (field: string, value: string) => {
  return `${field} = ${fixOperandFormat(value)}`;
};

/**
 * Translates MongoDB query statement
 * into SQL WHERE statement
 * @param expression: valid JSON5 parsed
 * MongoDB query object
 * @param logicOperator top-level logical operator, and
 * by default
 * @return valid SQL WHERE statement
 */
const mapQueryToSQL = (
  expression: JsonMap,
  logicOperator: LogicalOperator = LogicalOperator.AND,
): string => {
  const sqlConditions = [];
  // iterate over expression keys
  Object.keys(expression).forEach(field => {
    const value = expression[field];
    if (field.startsWith('$')) {
      sqlConditions.push(
        ...mapLogicalOperatorSQL(field, value as JsonArray, logicOperator),
      );
    } else if (typeof value == 'object') {
      // parse comparison operators
      sqlConditions.push(
        ...mapComparisonOperatorSQL(field, value as JsonMap, logicOperator),
      );
    } else {
      // equality operator case
      sqlConditions.push(mapEqualityOperatorToSQL(field, value as string));
    }
  });

  if (sqlConditions.length === 1) {
    return sqlConditions[0];
  }
  return joinResults(logicOperator, sqlConditions);
};

/**
 * Translates MongoDB projection into select query
 * @param values MongoDB projection map
 * @return sql select statement
 */
const mapProjectionToSQL = (values: { [key: string]: number }): string => {
  const fields = [];
  Object.keys(values).forEach(field => {
    const projection = values[field];
    // only support include projections
    if (projection === 1) {
      fields.push(field);
    }
  });
  return fields.join(', ');
};

/**
 * Extracts collection name and query from
 * valid MongoDB find query statement
 * @param mongoQuery
 */
const getCollectionNameAndQuery = (
  mongoQuery: string,
): { collection: string; queryWithoutFindPart: string } => {
  // remove db part
  const index = mongoQuery.indexOf('.') + 1;
  const queryWithoutDB = mongoQuery.slice(index);
  // extract collection and query
  const collection = queryWithoutDB.slice(0, queryWithoutDB.indexOf('.'));
  const query = queryWithoutDB.slice(queryWithoutDB.indexOf('.') + 1);
  const queryWithoutFindPart = query.slice(5, -2);
  return { collection, queryWithoutFindPart };
};

/**
 * validates if filter query is valid
 * @param parsedQuery parsed filter query object
 */
const isQueryValid = (parsedQuery: JsonMap): boolean => {
  return !!queryLanguage.accepts(EJSON.stringify(parsedQuery));
};

/**
 * Loads mongodb query and projection as JS object
 * @param query mongo db query and projection as string
 */
const parseQueryAsObject = (query: string) => {
  try {
    const queryAsArray = `[${query}]`;
    return parse(queryAsArray);
  } catch (e) {
    throw new Error('You passed invalid MongoDB query.');
  }
};

/**
 * Splits projection part from query and parse
 * @param query query with projection part
 */
const splitOnConditionAndProjection = (
  query: string,
): { condition: string; fields: string } => {
  const queryAsObject = parseQueryAsObject(query);

  // validate query semantic
  if (!isQueryValid(queryAsObject[0])) {
    throw new Error('You passed MongoDB query with invalid semantics.');
  }
  const condition = mapQueryToSQL(queryAsObject[0]);
  if (queryAsObject.length === 1) {
    return { condition, fields: SELECT_ALL_FIELDS };
  }
  return { condition, fields: mapProjectionToSQL(queryAsObject[1]) };
};

/**
 * Builds full SQL query from parts
 * @param collection MongoDB collection name used as SQL table name
 * @param fields parsed SELECT statement
 * @param condition parsed WHERE statement
 */
const prepareSQLStatement = (
  collection: string,
  fields: string,
  condition: string,
): string => {
  // truncate first level brackets if exists
  const conditionWithoutBrackets = condition.startsWith('(')
    ? condition.slice(1, -1)
    : condition;
  if (condition) {
    return `SELECT ${fields} FROM ${collection} WHERE ${conditionWithoutBrackets};`;
  }
  return `SELECT ${fields} FROM ${collection};`;
};

/**
 * Parses a MongoDB find query string and constructing SQL SELECT query
 * @param mongoQuery single MongoDB find query
 */
export const convert = (mongoQuery: string): string => {
  const formattedMongoQuery = checkAndFormatQuery(mongoQuery);
  const { collection, queryWithoutFindPart } =
    getCollectionNameAndQuery(formattedMongoQuery);
  const { condition, fields } =
    splitOnConditionAndProjection(queryWithoutFindPart);
  return prepareSQLStatement(collection, fields, condition);
};
