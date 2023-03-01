#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  uint32_t serialized_size = row.GetSerializedSize(schema_);
  if (serialized_size + 32 > PAGE_SIZE) {
    return false;
  }

  auto cur_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (cur_page == nullptr) {
    return false;
  }
  cur_page->WLatch();
  // Insert into the first page with enough space. If no such page exists, create a new page and insert into that.
  // INVARIANT: cur_page is WLatched if you leave the loop normally.
  while (!cur_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
    auto next_page_id = cur_page->GetNextPageId();
    // If the next page is a valid page,
    if (next_page_id != INVALID_PAGE_ID) {
      // Unlatch and unpin the current page.
      cur_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), false);
      // And repeat the process with the next page.
      cur_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
      cur_page->WLatch();
    } else {
      // Otherwise we have run out of valid pages. We need to create a new page.
      auto new_page = static_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      // If we could not create a new page,
      if (new_page == nullptr) {
        // Then life sucks and we abort the transaction.
        cur_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), false);
        return false;
      }
      // Otherwise we were able to create a new page. We initialize it now.
      new_page->WLatch();
      cur_page->SetNextPageId(next_page_id);
      new_page->Init(next_page_id, cur_page->GetTablePageId(), log_manager_, txn);
      cur_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), true);
      cur_page = new_page;
    }
  }
  cur_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), true);
  return true;
}
bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Update the tuple; but first save the old value for rollbacks.
  Row old_row(rid);
  page->WLatch();
  bool is_updated = page->UpdateTuple(row, &old_row , schema_,txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), is_updated);
  return is_updated;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  ASSERT(page != nullptr, "Couldn't find a page containing that RID.");
  // Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  delete page;
  page_id_t nextpage=page->GetNextPageId() ;
  page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(nextpage));
  while (page != nullptr) delete page;
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Read the tuple from the page.
  page->RLatch();
  bool res = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
  return res;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  RowId rid;
  auto page_id = first_page_id_;
  while (page_id != INVALID_PAGE_ID) {
    auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    page->RLatch();
    // If this fails because there is no tuple, then RID will be the default-constructed value, which means EOF.
    auto found_tuple = page->GetFirstTupleRid(&rid);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false);
    if (found_tuple) {
      break;
    }
    page_id = page->GetNextPageId();
  }
  return TableIterator(this, rid, txn);
}

TableIterator TableHeap::End() {
  return TableIterator(this, RowId(INVALID_PAGE_ID,0), nullptr);
}
