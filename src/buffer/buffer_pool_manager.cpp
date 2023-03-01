#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
    // 1.     Search the page table for the requested page (P).
    // 1.1    If P exists, pin it and return it immediately.
    // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
    //        Note that pages are always found from the free list first.
    // 2.     If R is dirty, write it back to the disk.
    // 3.     Delete R from the page table and insert P.
    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    unordered_map<page_id_t, frame_id_t> ::iterator page_it;
    page_it = page_table_.find(page_id);
    if(page_it != page_table_.end())
    {
        replacer_->Pin(page_it->second);
        pages_[page_it->second].pin_count_++;
        return &pages_[page_it->second];
    }//search the table

    else
    {
        frame_id_t targetR;
        if(!free_list_.empty())
        {
            targetR = free_list_.front();
            free_list_.pop_front();
        }
        else if(!replacer_->Victim(&targetR))
            return nullptr;
        page_it = page_table_.begin();
        while(page_it!=page_table_.end())
        {
            if (page_it->second == targetR)
                break;
            else
                page_it++;
        }
        //得到符合要求的page
        if(pages_[targetR].IsDirty())
            disk_manager_->WritePage(page_it->first,pages_[targetR].GetData());
        disk_manager_->ReadPage(page_id,pages_[targetR].data_);
        pages_[targetR].page_id_ = page_id;
        pages_[targetR].is_dirty_ = true;
        pages_[targetR].pin_count_ = 1;
        page_table_.insert(pair<page_id_t, frame_id_t>(page_id,targetR));
        if(page_it!=page_table_.end()){
            page_table_.erase(page_it);
        }
        //uint32_t *data = reinterpret_cast<uint32_t *>(disk_manager_->GetMetaData());

        return &pages_[targetR];
    }

    return nullptr;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
    // 0.   Make sure you call AllocatePage!
    // 1.   If all the pages in the buffer pool are pinned, return nullptr.
    // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
    // 3.   Update P's metadata, zero out memory and add P to the page table.
    // 4.   Set the page ID output parameter. Return a pointer to P.
    size_t i;
    for (i = 0; i < pool_size_; ++i) {
        if(pages_[i].pin_count_ == 0)
            break ;
    }
    if(i == pool_size_)
        return nullptr;
    else
    {
        unordered_map<page_id_t, frame_id_t> ::iterator page_it;
        page_id = disk_manager_->AllocatePage();
        frame_id_t targetR;
        if(!free_list_.empty()) {targetR = free_list_.front();free_list_.pop_front();}
        else if(!replacer_->Victim(&targetR)) return nullptr;
        page_it = page_table_.begin();
        while(page_it!=page_table_.end()){
            if (page_it->second == targetR)
                break;
            else
                page_it++;
        }
        //得到符合要求的page
        if(pages_[targetR].IsDirty())
            disk_manager_->WritePage(page_it->first,pages_[targetR].GetData());
        pages_[targetR].ResetMemory();
        pages_[targetR].page_id_ = page_id;
        pages_[targetR].is_dirty_ = false;
        pages_[targetR].pin_count_ = 1;
        page_table_.insert(pair<page_id_t, frame_id_t>(page_id,targetR));
        if(page_it!=page_table_.end()){
            page_table_.erase(page_it);
        }
        return &pages_[targetR];
    }
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
    // 0.   Make sure you call DeallocatePage!
    // 1.   Search the page table for the requested page (P).
    // 1.   If P does not exist, return true.
    // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
    unordered_map<page_id_t, frame_id_t> ::iterator page_it;
    page_it = page_table_.find(page_id);
    if(page_it == page_table_.end())
        return true;
    if(pages_[page_it->second].pin_count_!=0)
        return false;
    pages_[page_it->second].ResetMemory();
    pages_[page_it->second].pin_count_=0;
    pages_[page_it->second].page_id_ = INVALID_PAGE_ID;
    pages_[page_it->second].is_dirty_ = false;
    disk_manager_->DeAllocatePage(page_id);
    free_list_.push_front(page_it->second);
    page_table_.erase(page_it);
    return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty)
{
    unordered_map<page_id_t, frame_id_t> ::iterator page_it;
    page_it = page_table_.find(page_id);
    if (page_it == page_table_.end() || page_it->first<0) 
        return false;
    if(is_dirty)
        disk_manager_->WritePage(page_it->first,pages_[page_it->second].GetData());
    pages_[page_it->second].pin_count_ = 0;
    replacer_->Unpin(page_it->second);
    return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
    size_t i;
    for(i = 0;i<pool_size_;i++)
    {
      if (pages_[i].page_id_ < 0) return false;
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].GetData());
    }
    if(i == pool_size_) return true;
    else return false;
}

page_id_t BufferPoolManager::AllocatePage() {
    int next_page_id = disk_manager_->AllocatePage();
    return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
    disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
    return disk_manager_->IsPageFree(page_id);
}


// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}