#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"


class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  explicit TableIterator(TableHeap *table_heap, RowId rid, Transaction *txn);

  explicit TableIterator(const TableIterator &other)
      : table_heap_(other.table_heap_), row_(new Row(*other.row_)), txn_(other.txn_) {}

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

  TableIterator &operator=(const TableIterator &other) {
    table_heap_ = other.table_heap_;
    row_ = (other.row_);
    txn_ = other.txn_;
    return *this;
  }

private:
 TableHeap *table_heap_;
 Row *row_;
 Transaction *txn_;
 // add your own private member variables here
};

#endif //MINISQL_TABLE_ITERATOR_H
