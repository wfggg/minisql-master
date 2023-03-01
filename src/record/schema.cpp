#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t ofs = 0;
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM); 
  ofs = ofs + sizeof(uint32_t);
  buf = buf + sizeof(uint32_t);
  size_t columncount = GetColumnCount();
  if (columncount == 0) return 0;
  uint32_t nullcount = 0;
  MACH_WRITE_TO(size_t, buf, columncount);
  ofs = ofs + sizeof(size_t);
  buf = buf + sizeof(size_t);
  const Column *temp;

  for (size_t i = 0; i < columncount; i++) {
    temp = GetColumn(i);
    if (temp == nullptr) {
      nullcount++;
    }
  }
  if (nullcount == columncount) return ofs;
  for (size_t i = 0; i < columncount; i++) {
    temp = GetColumn(i);
    if (temp != nullptr) {
      temp->SerializeTo(buf);
      ofs = ofs + temp->GetSerializedSize();
      buf = buf + temp->GetSerializedSize();
    }
  }
  return ofs;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t ofs = 0;
  ofs = ofs + sizeof(uint32_t);
  size_t columncount = GetColumnCount();
  if (columncount == 0) return 0;
  uint32_t nullcount = 0;
  ofs = ofs + sizeof(size_t);
  const Column *temp;

  for (size_t i = 0; i < columncount; i++) {
    temp = GetColumn(i);
    if (temp == nullptr) {
      nullcount++;
    }
  }
  if (nullcount == columncount) return ofs;
  for (size_t i = 0; i < columncount; i++) {
    temp = GetColumn(i);
    if (temp != nullptr) {
      ofs = ofs + temp->GetSerializedSize();
    }
  }
  return ofs;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) { 
  uint32_t ofs = 0;
  std::vector<Column *> columns;
  uint32_t _SCHEMA_MAGIC_NUM = MACH_READ_UINT32(buf);
  ofs = ofs + sizeof(uint32_t);
  buf = buf + sizeof(uint32_t);
  if (_SCHEMA_MAGIC_NUM != SCHEMA_MAGIC_NUM) {
    return 0;
  }
  size_t columncount = MACH_READ_FROM(size_t, buf);
  ofs = ofs + sizeof(size_t);
  buf = buf + sizeof(size_t);
  for (size_t j = 0; j < columncount; j++) {
    Column *col;
    uint32_t ofss = Column::DeserializeFrom(buf, col, heap);
    ofs = ofs + ofss;
    columns.push_back(col);
  }
  void *mem = heap->Allocate(sizeof(Schema));
  schema = new (mem) Schema(columns);
  return ofs; 
}