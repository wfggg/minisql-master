#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Transaction *txn)
    : table_heap_(table_heap), row_(new Row(rid)), txn_(txn) {
  if (rid.GetPageId() != INVALID_PAGE_ID) {
    table_heap_->GetTuple(row_, txn_);
  }
}

TableIterator::~TableIterator() {
  delete row_; 
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return row_->GetRowId().Get() == itr.row_->GetRowId().Get();
}

bool TableIterator::operator!=(const TableIterator &itr) const { return !(*this == itr); }

const Row &TableIterator::operator*() {
  assert(*this != table_heap_->End());
  return *row_;
}

Row *TableIterator::operator->() {
  assert(*this != table_heap_->End());
  return row_;
}

TableIterator &TableIterator::operator++() {
  BufferPoolManager *buffer_pool_manager = table_heap_->buffer_pool_manager_;
  auto cur_page = static_cast<TablePage *>(buffer_pool_manager->FetchPage(row_->GetRowId().GetPageId()));
  cur_page->RLatch();
  assert(cur_page != nullptr);  // all pages are pinned

  RowId next_tuple_rid;
  if (!cur_page->GetNextTupleRid(row_->GetRowId(),
                                 &next_tuple_rid)) {  // end of this page
    while (cur_page->GetNextPageId() != INVALID_PAGE_ID) {
      auto next_page = static_cast<TablePage *>(buffer_pool_manager->FetchPage(cur_page->GetNextPageId()));
      cur_page->RUnlatch();
      buffer_pool_manager->UnpinPage(cur_page->GetTablePageId(), false);
      cur_page = next_page;
      cur_page->RLatch();
      if (cur_page->GetFirstTupleRid(&next_tuple_rid)) {
        break;
      }
    }
  }
  row_->SetRowId(next_tuple_rid);

  if (*this != table_heap_->End()) {
    table_heap_->GetTuple(row_,txn_);
  }
  // release until copy the tuple
  cur_page->RUnlatch();
  buffer_pool_manager->UnpinPage(cur_page->GetTablePageId(), false);
  return *this;
}

TableIterator TableIterator::operator++(int) {
  ++(*this);
  return TableIterator(*this);
}
