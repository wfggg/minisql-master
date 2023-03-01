#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  // ASSERT(false, "Not Implemented yet");
  uint32_t move = 0;  // the offset based on buf
  // store the magic_num into buffer
  MACH_WRITE_TO(uint32_t, buf, CATALOG_METADATA_MAGIC_NUM);
  uint32_t len_magic = sizeof(uint32_t);
  move += len_magic;
  uint32_t size_map1 = table_meta_pages_.size();
  uint32_t len_ele_map1 = sizeof(table_id_t) + sizeof(page_id_t);
  // store the size of table_meta_pages_ into buffer
  MACH_WRITE_TO(uint32_t, buf + move, size_map1 * len_ele_map1);
  move += sizeof(uint32_t);
  // std::map<table_id_t,page_id_t>::iterator it;
  // store the elements of table_meta_pages_ into buffer
  // for(it=table_meta_pages_.begin();it!=table_meta_pages_.end();it++)
  for (auto it : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf + move, it.first);
    move += sizeof(table_id_t);
    MACH_WRITE_TO(page_id_t, buf + move, it.second);
    move += sizeof(page_id_t);
  }

  uint32_t size_map2 = index_meta_pages_.size();
  uint32_t len_ele_map2 = sizeof(index_id_t) + sizeof(page_id_t);
  // store the size of the index_meta_pages_ into buffer
  MACH_WRITE_TO(uint32_t, buf + move, size_map2 * len_ele_map2);
  move += sizeof(uint32_t);
  // std::map<index_id_t,page_id_t>::iterator it2;
  // store the elements of index_meta_pages_ into buffer
  // for(it2=index_meta_pages_.begin();it2!=index_meta_pages_.end();it2++)
  for (auto it2 : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf + move, it2.first);
    move += sizeof(index_id_t);
    MACH_WRITE_TO(page_id_t, buf + move, it2.second);
    move += sizeof(page_id_t);
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // ASSERT(false, "Not Implemented yet");
  uint32_t move = 0;
  // read the magic_num back
  uint32_t val = MACH_READ_FROM(uint32_t, buf);
  move += sizeof(uint32_t);
  CatalogMeta *ptr = ALLOC_P(heap, CatalogMeta);
  if (val != CATALOG_METADATA_MAGIC_NUM) return nullptr;
  // read the elements of map1 back
  uint32_t len_ele_map1 = sizeof(table_id_t) + sizeof(page_id_t);
  uint32_t len_map1 = MACH_READ_FROM(uint32_t, buf + move);
  move += sizeof(uint32_t);
  uint32_t size_map1 = len_map1 / len_ele_map1;
  table_id_t first1;
  page_id_t second1;
  for (uint32_t i = 0; i < size_map1; i++) {
    first1 = MACH_READ_FROM(table_id_t, buf + move);
    move += sizeof(table_id_t);
    second1 = MACH_READ_FROM(page_id_t, buf + move);
    move += sizeof(page_id_t);
    ptr->table_meta_pages_.insert(pair<table_id_t, page_id_t>(first1, second1));
  }

  // read the elements of map2 back
  uint32_t len_ele_map2 = sizeof(index_id_t) + sizeof(page_id_t);
  uint32_t len_map2 = MACH_READ_FROM(uint32_t, buf + move);
  move += sizeof(uint32_t);
  uint32_t size_map2 = len_map2 / len_ele_map2;
  index_id_t first2;
  page_id_t second2;
  for (uint32_t i = 0; i < size_map2; i++) {
    first2 = MACH_READ_FROM(index_id_t, buf + move);
    move += sizeof(index_id_t);
    second2 = MACH_READ_FROM(page_id_t, buf + move);
    move += sizeof(page_id_t);
    ptr->index_meta_pages_.insert(pair<index_id_t, page_id_t>(first2, second2));
  }
  return ptr;  // origin: return nullptr;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  uint32_t sum = 0;
  sum += sizeof(uint32_t);  // for magic_num;
  uint32_t table_size = sizeof(table_id_t) + sizeof(page_id_t);
  sum += table_size * table_meta_pages_.size();  // for length of map1
  uint32_t index_size = sizeof(index_id_t) + sizeof(page_id_t);
  sum += index_size * index_meta_pages_.size();  // for length of map2
  return sum;
}

CatalogMeta::CatalogMeta() {
  // with no parameter, just initialize members as default
  table_meta_pages_.clear();
  index_meta_pages_.clear();
}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager),
      lock_manager_(lock_manager),
      log_manager_(log_manager),
      heap_(new SimpleMemHeap()) {
  // ASSERT(false, "Not Implemented yet");
  if (init == true) {
    /*Page *catalog_page=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    char *buf=catalog_page->GetData();*/

    catalog_meta_ = CatalogMeta::NewInstance(heap_);
    // catalog_meta_->SerializeTo(buf);
    /*buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,false);*/
  } else {
    char *buf = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID)->GetData();
    catalog_meta_ = CatalogMeta::DeserializeFrom(buf, heap_);

    /*tables_.clear();
    table_names_.clear();
    indexes_.clear();
    index_names_.clear();*/

    // std::map<table_id_t,page_id_t>::iterator it1;
    // for(it1=catalog_meta_->table_meta_pages_.begin();it1!=catalog_meta_->table_meta_pages_.end();it1++)
    for (auto it1 : catalog_meta_->table_meta_pages_) {
      table_id_t table_id = it1.first;
      page_id_t page_id1 = it1.second;
      LoadTable(table_id, page_id1);
    }

    std::map<index_id_t, page_id_t>::iterator it2;
    for (it2 = catalog_meta_->index_meta_pages_.begin(); it2 != catalog_meta_->index_meta_pages_.end(); it2++) {
      index_id_t index_id = it2->first;
      page_id_t page_id2 = it2->second;
      LoadIndex(index_id, page_id2);
    }
  }
}

CatalogManager::~CatalogManager() { delete heap_; }

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  // judge if the table is existed
  std::unordered_map<std::string, table_id_t>::iterator it1;
  it1 = table_names_.find(table_name);
  if (it1 != table_names_.end()) return DB_TABLE_ALREADY_EXIST;
  // Create at first
  table_info = TableInfo::Create(heap_);
  // init with table_heap & table_meta
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, heap_);
  table_id_t table_id = next_table_id_++;
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema, heap_);
  Page *page = buffer_pool_manager_->FetchPage(table_heap->GetFirstPageId());
  char *buf = page->GetData();
  table_meta->SerializeTo(buf);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  table_info->Init(table_meta, table_heap);
  // update about tables in the map
  table_names_.emplace(table_name, table_id);
  tables_.emplace(table_id, table_info);
  std::unordered_map<std::string, index_id_t> index_name;
  index_name.clear();
  index_names_.emplace(table_name, index_name);
  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  std::unordered_map<std::string, table_id_t>::iterator it;
  it = this->table_names_.find(table_name);
  if (it == this->table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = it->second;
  std::unordered_map<table_id_t, TableInfo *>::iterator it1;
  it1 = this->tables_.find(table_id);
  if (it1 == this->tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  TableInfo *tmp = it1->second;
  table_info = tmp;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  // std::unordered_map<table_id_t,TableInfo *>::iterator it;
  // for(it=tables_.begin();it!=tables_.end();it++)
  for (auto it : tables_) {
    tables.push_back(it.second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // ASSERT(false, "Not Implemented yet");
 // First of all, get the right TableInfo
// find the table_id_ in table_names_
std::unordered_map<std::string, table_id_t>::iterator it1;
  it1 = table_names_.find(table_name);
  if (it1 == table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it0 = index_names_[table_name];
  if (it0.count(index_name) > 0) return DB_INDEX_ALREADY_EXIST;
  // get TableInfo using table_id_ above
  TableInfo *table_info;
  this->GetTable(it1->second, table_info);
  // Second, put the new index in
  // get the column_id represented by index_keys
  std::vector<uint32_t> key_map;
  // std::vector<std::string>::iterator it2;
  // for(it2=index_keys.begin();it2!=index_keys.end();it2++)
  for (auto it2 : index_keys) {
    uint32_t col_index;
    // GetColumnIndex取出来的是it2代表的column名在columns_[]中的下标，放在col_index中
    if (table_info->GetSchema()->GetColumnIndex(it2, col_index) != DB_COLUMN_NAME_NOT_EXIST) {
      key_map.push_back(col_index);
    } else
      return DB_COLUMN_NAME_NOT_EXIST;
  }
  // insert or update in index_names_
  std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>>::iterator it3;
  it3 = index_names_.find(table_name);
  if (it3 != index_names_.end()) {
    std::unordered_map<std::string, index_id_t> temp;
    temp = it3->second;
    if (temp.find(index_name) != temp.end()) return DB_INDEX_ALREADY_EXIST;
    it3->second.insert(pair<std::string, index_id_t>(index_name, next_index_id_));
    next_index_id_++;
  } else {
    std::unordered_map<std::string, index_id_t> index_map;
    index_map.insert(pair<std::string, index_id_t>(index_name, next_index_id_));
    next_index_id_++;
    index_names_.insert(pair<std::string, std::unordered_map<std::string, index_id_t>>(table_name, index_map));
  }
  // create the IndexInfo
  index_info = IndexInfo::Create(heap_);
  // create index_metadata and serialize it
  IndexMetadata *index_metadata = IndexMetadata::Create(next_index_id_, index_name, it1->second, key_map, heap_);
  page_id_t page_id;
  index_metadata->SerializeTo(buffer_pool_manager_->NewPage(page_id)->GetData());
  // after metadata, init indexInfo
  index_info->Init(index_metadata, table_info, buffer_pool_manager_);
  // update indexes
  indexes_.insert(pair<index_id_t, IndexInfo *>(next_index_id_, index_info));
  // serialize catalog_meta back to page
  catalog_meta_->index_meta_pages_.insert(pair<index_id_t, page_id_t>(next_index_id_, page_id));
  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  // get the right table
  // std::unordered_map<std::string,std::unordered_map<std::string, index_id_t>>::iterator it1;
  auto it1 = index_names_.find(table_name);
  if (it1 == index_names_.end()) return DB_TABLE_NOT_EXIST;
  std::unordered_map<std::string, index_id_t> index_map = it1->second;
  // get the right index_id
  // std::unordered_map<std::string,index_id_t>>::iterator it2;
  auto it2 = index_map.find(index_name);
  if (it2 == index_map.end()) return DB_INDEX_NOT_FOUND;
  index_id_t index_id = it2->second;
  // get info
  // std::unordered_map<index_id_t,IndexInfo *>::iterator it3;
  auto it3 = indexes_.find(index_id);
  if (it3 == indexes_.end()) return DB_INDEX_NOT_FOUND;
  index_info = it3->second;

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  // get the right table
  // std::unordered_map<std::string,std::unordered_map<std::string, index_id_t>>::iterator it1;
  auto it1 = index_names_.find(table_name);
  if (it1 == index_names_.end()) return DB_TABLE_NOT_EXIST;
  std::unordered_map<std::string, index_id_t> index_map = it1->second;
  // get all the indexes in this table
  std::unordered_map<std::string, index_id_t>::iterator it2;
  for (it2 = index_map.begin(); it2 != index_map.end(); it2++) {
    index_id_t index_id = it2->second;
    // std::unordered_map<index_id_t,IndexInfo *>::iterator it3;
    auto it3 = indexes_.find(index_id);
    if (it3 == indexes_.end()) return DB_INDEX_NOT_FOUND;
    indexes.push_back(it3->second);
  }

  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  // find id of the table in the table_names_
  std::unordered_map<std::string, table_id_t>::iterator it1;
  it1 = table_names_.find(table_name);
  if (it1 == table_names_.end()) return DB_TABLE_NOT_EXIST;
  // erase the table in the maps
  table_id_t table_id = it1->second;
  table_names_.erase(table_name);
  tables_.erase(table_id);
  index_names_.erase(table_name);
  // flush the page
  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  // judge if table is existed
  std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>>::iterator it1;
  it1 = index_names_.find(table_name);
  if (it1 == index_names_.end()) return DB_TABLE_NOT_EXIST;
  // judge if index is existed
  std::unordered_map<std::string, index_id_t>::iterator it2;
  it2 = it1->second.find(index_name);
  if (it2 == it1->second.end()) return DB_INDEX_NOT_FOUND;
  // update the index_names_ and indexes_
  index_id_t index_id = it2->second;
  it1->second.erase(index_name);
  indexes_.erase(index_id);
  // flush
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  // update the map of pages within the CatalogMeta
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  // std::unordered_map<table_id_t,TableInfo *>::iterator it1;
  // for(it1=tables_.begin();it1!=tables_.end();it1++)
  for (auto it1 : tables_)
    catalog_meta_->table_meta_pages_.emplace(pair<table_id_t, page_id_t>(it1.first, it1.second->GetRootPageId()));
  // std::unordered_map<index_id_t,IndexInfo *>::iterator it2;
  // for(it2=indexes_.begin();it2!=indexes_.end();it2++)
  for (auto it2 : indexes_)
    catalog_meta_->index_meta_pages_.emplace(
        pair<index_id_t, page_id_t>(it2.first, it2.second->GetTableInfo()->GetRootPageId()));
  // serialize to the page of catalog_meta and set to be dirty
  char *buf = catalog_meta_page->GetData();
  catalog_meta_->SerializeTo(buf);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  // build tableinfo and tablepage for load eventually
  Page *tablepage = buffer_pool_manager_->FetchPage(page_id);
  TableInfo *tableinfo = TableInfo::Create(heap_);
  // get data for deserialization
  char *buf = tablepage->GetData();
  // MemHeap *memheap=tableinfo->GetMemHeap();
  //  deserialize
  TableMetadata *tablemeta;
  // tablemeta=(TableMetadata *)malloc(sizeof(TableMetadata));
  tablemeta->DeserializeFrom(buf, tablemeta, heap_);
  // page is not dirty anymore
  // buffer_pool_manager_->UnpinPage(page_id,false);
  // get tableheap
  TableHeap *tableheap;
  tableheap =
      TableHeap::Create(buffer_pool_manager_, tablemeta->GetSchema(), nullptr, log_manager_, lock_manager_, heap_);
  // initialize tableinfo
  tableinfo->Init(tablemeta, tableheap);
  table_names_[tablemeta->GetTableName()] = tablemeta->GetTableId();
  tables_[tablemeta->GetTableId()] = tableinfo;
  index_names_.insert({tablemeta->GetTableName(), std::unordered_map<std::string, index_id_t>()});
  /*std::unordered_map<table_id_t,TableInfo *>::iterator it1;
  it1=tables_.find(table_id);
  if(it1==tables_.end())
  {
    tables_.emplace(make_pair(table_id,tableinfo));

    table_names_.emplace(make_pair(tablemeta->GetTableName(),table_id));
    index_names_.insert({tablemeta->GetTableName(), std::unordered_map<std::string, index_id_t>()});
  }
  else
  {
    return DB_TABLE_ALREADY_EXIST;
  }*/

  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  // build indexinfo and indexpage for load eventually
  Page *indexpage = buffer_pool_manager_->FetchPage(page_id);
  IndexInfo *indexinfo = IndexInfo::Create(heap_);
  // get data for deserialization
  char *buf = indexpage->GetData();
  // MemHeap *memheap=indexinfo->GetMemHeap();
  // deserialize
  IndexMetadata *indexmeta;
  IndexMetadata::DeserializeFrom(buf, indexmeta, heap_);
  // page is not dirty anymore
  // buffer_pool_manager_->UnpinPage(page_id,false);
  // get table_info
  table_id_t tableid = indexmeta->GetTableId();
  std::unordered_map<table_id_t, TableInfo *>::iterator it1 = tables_.find(tableid);
  TableInfo *tableinfo = it1->second;
  std::string tablename = tableinfo->GetTableName();
  // initialize indexinfo
  indexinfo->Init(indexmeta, tableinfo, buffer_pool_manager_);
  // indexes_.emplace(make_pair(index_id,indexinfo));
  indexes_[indexmeta->GetIndexId()] = indexinfo;
  index_names_[tableinfo->GetTableName()][indexmeta->GetIndexName()] = indexmeta->GetIndexId();
  /*std::pair<std::string,index_id_t> index_name_pair(indexmeta->GetIndexName(),index_id);
  std::unordered_map<std::string,index_id_t> index_name;
  index_name.emplace(index_name_pair);
  std::unordered_map<std::string,std::unordered_map<std::string,index_id_t>>::iterator it2;
  it2=index_names_.find(tablename);*/
  // this table has been existed
  /*if(it2!=index_names_.end())
  {
    index_names_.emplace(make_pair(tablename,index_name));
    return DB_TABLE_ALREADY_EXIST;
  }
  else
  {
    it2->second.emplace(index_name_pair);
    return DB_TABLE_NOT_EXIST;
  }*/
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  // get table_info directly
  std::unordered_map<table_id_t, TableInfo *>::iterator it1;
  it1 = tables_.find(table_id);
  if (it1 == tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = it1->second;

  return DB_SUCCESS;
 } 