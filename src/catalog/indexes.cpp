#include "catalog/indexes.h"
IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name, const table_id_t table_id,
                                     const vector<uint32_t> &key_map, MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new (buf) IndexMetadata(index_id, index_name, table_id, key_map);
}
uint32_t IndexMetadata::SerializeTo(char *buf) const {
  uint32_t move = 0;
MACH_WRITE_TO(uint32_t, buf, INDEX_METADATA_MAGIC_NUM);
 uint32_t len_magic = sizeof(uint32_t);
   move += len_magic;
MACH_WRITE_TO(index_id_t, buf + move, index_id_);
   uint32_t len_index_id = sizeof(index_id_t);
   move += len_index_id;
 uint32_t len_name = index_name_.size();
   MACH_WRITE_TO(uint32_t, buf + move, len_name);
   move += sizeof(uint32_t);
   MACH_WRITE_STRING(buf + move, index_name_);
   move += len_name * sizeof(char);
 MACH_WRITE_TO(table_id_t, buf + move, table_id_);
   uint32_t len_table_id = sizeof(table_id_t);
   move += len_table_id;
 uint32_t size_key = key_map_.size();
   uint32_t len_key = size_key * sizeof(uint32_t);
   MACH_WRITE_TO(uint32_t, buf + move, len_key);
   move += sizeof(uint32_t);
   std::vector<uint32_t>::iterator it;
 for (auto it : key_map_)
 {
     MACH_WRITE_TO(uint32_t, buf + move, it);
     move += sizeof(uint32_t);
    
  }
   return 0;
}

uint32_t IndexMetadata::GetSerializedSize() const {
   uint32_t sum = 0;
   sum += sizeof(uint32_t);
   sum += sizeof(index_id_t);
   uint32_t length = index_name_.size();
   sum += length * sizeof(char);
   sum += sizeof(table_id_t);
   uint32_t size = key_map_.size();
   sum += size * sizeof(uint32_t);

   return sum;
}
uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  uint32_t move = 0;
uint32_t val = MACH_READ_FROM(uint32_t, buf);
  if (val != index_meta->INDEX_METADATA_MAGIC_NUM) return 0;
  move += sizeof(uint32_t);
index_id_t index_id = MACH_READ_FROM(index_id_t, buf + move);
 move += sizeof(index_id_t);
 uint32_t len_name = MACH_READ_FROM(uint32_t, buf + move);
  move += sizeof(uint32_t);
uint32_t size_name = len_name / sizeof(char);
  string index_name;
  for (uint32_t i = 0; i < size_name; i++)
{
   
index_name.append(1, MACH_READ_FROM(char, buf + move));
     move += sizeof(char);
 
  }
table_id_t table_id = MACH_READ_FROM(table_id_t, buf + move);
   move += sizeof(table_id_t);
 uint32_t len_key = MACH_READ_FROM(uint32_t, buf + move);
   move += sizeof(uint32_t);
   uint32_t size_key = len_key / sizeof(uint32_t);
   std::vector<uint32_t> key_map;
   for (uint32_t i = 0; i < size_key; i++)
 {
    key_map.push_back(MACH_READ_FROM(uint32_t, buf + move));
     move += sizeof(uint32_t);
    
  }
   index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map, heap);
   return 0;
}