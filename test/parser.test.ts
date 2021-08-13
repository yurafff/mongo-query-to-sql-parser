import { expect } from 'chai';
import { convert } from '../lib/parser';

// set of valid mongodb queries to test
const VALID_MONGO_TEST_QUERIES: { mongoQuery: string; sqlQuery: string }[] = [
  {
    mongoQuery:
      'db.inventory.find({ $or: [ { quantity: { $lt: 20 } }, { price: 10 } ] });',
    sqlQuery: 'SELECT * FROM inventory WHERE quantity < 20 OR price = 10;',
  },
  {
    mongoQuery: 'db.inventory.find({ quantity: { $in: [20, 50] } })',
    sqlQuery: 'SELECT * FROM inventory WHERE quantity IN (20, 50);',
  },
  {
    mongoQuery:
      'db.inventory.find({$and:[{$or:[{qty:{$lt:10}},{qty:{$gt:50}}]},{$or:[{sale:1},{price:{$gt:5}}]}]});',
    sqlQuery:
      'SELECT * FROM inventory WHERE (qty < 10 OR qty > 50) AND (sale = 1 OR price > 5);',
  },
  {
    mongoQuery: 'db.inventory.find( { qty: { $gt: 20 } } )',
    sqlQuery: 'SELECT * FROM inventory WHERE qty > 20;',
  },
  {
    mongoQuery: 'db.inventory.find( { qty: { $lte: 20 } } );',
    sqlQuery: 'SELECT * FROM inventory WHERE qty <= 20;',
  },
  {
    mongoQuery: 'db.inventory.find( { qty: 20 } );',
    sqlQuery: 'SELECT * FROM inventory WHERE qty = 20;',
  },
  {
    mongoQuery:
      "db.inventory.find({ $or: [{ status: 'A' }, { qty: { $lt: 30 } }] })",
    sqlQuery: "SELECT * FROM inventory WHERE status = 'A' OR qty < 30;",
  },
  {
    mongoQuery:
      'db.inventory.find({inventorySku: { $gte:21 }, $or: [ { qty: { $lt: 20 } }, { price: 10 } ]},{inventoryName:1,_id:1})',
    sqlQuery:
      'SELECT inventoryName, _id FROM inventory WHERE inventorySku >= 21 AND (qty < 20 OR price = 10);',
  },
  {
    mongoQuery:
      "db.employee.find({age: {$gte: 46}, name: 'Yury', skills: { $in: [ 'TypeScript', 'MongoDB' ]}},{age: 1, name: 1, _id: 1})",
    sqlQuery:
      "SELECT age, name, _id FROM employee WHERE age >= 46 AND name = 'Yury' AND skills IN ('TypeScript', 'MongoDB');",
  },
  {
    mongoQuery: "db.advertisement.find({advertiser: 'Zara'});",
    sqlQuery: "SELECT * FROM advertisement WHERE advertiser = 'Zara';",
  },
  {
    mongoQuery:
      'db.advertisement.find({_id: 111112222},{title: 1, advertiser: 1});',
    sqlQuery:
      'SELECT title, advertiser FROM advertisement WHERE _id = 111112222;',
  },
  {
    mongoQuery:
      'db.advertisement.find({ $or: [ { likes: { $lt: 30 } }, { userScore: 55 } ]});',
    sqlQuery: 'SELECT * FROM advertisement WHERE likes < 30 OR userScore = 55;',
  },
];

// not valid query cases
const NOT_FIND_QUERY: string =
  'db.products.insert( { item: "card", qty: 15 } );';
const QUERY_WITH_INVALID_OPERATOR =
  'db.bios.find( { age: { $gt: 7 }, death: { $exists: false } } )';

describe('Integration test of module main function', () => {
  it('Should translate MongoDB find queries into SQL', () => {
    VALID_MONGO_TEST_QUERIES.forEach(mockedQuery => {
      let outputString;
      try {
        outputString = convert(mockedQuery.mongoQuery);
      } catch (error) {
        throw new Error(error);
      }
      expect(outputString).equal(mockedQuery.sqlQuery);
    });
  });

  it('Should raise error on not find queries', () => {
    expect(() => {
      convert(NOT_FIND_QUERY);
    }).to.throw('Only find queries supported');
  });

  it('Should raise error invalid query operator', () => {
    expect(() => {
      convert(QUERY_WITH_INVALID_OPERATOR);
    }).to.throw('MongoDB operator: $exists not supported.');
  });
});
