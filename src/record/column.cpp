#include "record/column.h"
#include <iostream>
Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length), 
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t ofs=0;
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  ofs = ofs + sizeof(uint32_t);
  buf = buf + sizeof(uint32_t);
  uint32_t name_lenth = name_.size();  //处理name_
  memcpy(buf, &name_lenth, sizeof(uint32_t));
  ofs += sizeof(uint32_t);
  buf += sizeof(uint32_t);
  // memcpy(pos, &name_, name_len);
  name_.copy(buf, name_lenth, 0);
  ofs += name_lenth;
  buf += name_lenth;
  uint8_t stype;  //处理TypeId type_
  switch (type_) {
    case kTypeInvalid: {
      stype = 0;
      break;
    }
    case kTypeInt: {
      stype = 1;
      break;
    }
    case kTypeFloat: {
      stype = 2;
      break;
    }
    case kTypeChar: {
      stype = 3;
      break;
    }
    default: {
      stype = 0;
      break;
    }
  }
  memcpy(buf, &stype, sizeof(uint8_t));
  ofs += sizeof(uint8_t);
  buf += sizeof(uint8_t);
  memcpy(buf, &len_, sizeof(uint32_t));  // len_, for char
  ofs += sizeof(uint32_t);
  buf += sizeof(uint32_t);
  memcpy(buf, &table_ind_, sizeof(uint32_t));  // table_ind_, index
  ofs += sizeof(uint32_t);
  buf += sizeof(uint32_t);
  memcpy(buf, &nullable_, sizeof(bool));
  ofs += sizeof(bool);
  buf += sizeof(bool);
  memcpy(buf, &unique_, sizeof(bool));
  ofs += sizeof(bool);
  buf += sizeof(bool);
  return ofs;
}

uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  uint32_t ofs = 0;
  ofs = ofs + sizeof(uint32_t) * 4 + sizeof(uint8_t) + sizeof(bool) * 2 + name_.size();
  return ofs;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  // replace with your code here

  /* deserialize field from buf */
  uint32_t ofs=0;  //取出name_
  uint32_t _SCHEMA_MAGIC_NUM = MACH_READ_UINT32(buf);
  ofs = ofs + sizeof(uint32_t);
  buf = buf + sizeof(uint32_t);
  if (_SCHEMA_MAGIC_NUM != COLUMN_MAGIC_NUM) {
    return 0;
  }
  uint32_t name_len = MACH_READ_UINT32(buf);
 buf += sizeof(uint32_t);
 ofs += sizeof(uint32_t);
 std::string column_name;
 column_name.append(buf, name_len);
 buf += name_len;
 ofs += name_len;
 uint8_t stype = MACH_READ_FROM(uint8_t, buf);
 TypeId type;
 buf += sizeof(uint8_t);
 ofs += sizeof(uint8_t);
 switch (stype) {
   case 0: {
     type = kTypeInvalid;
     break;
   }
   case 1: {
     type = kTypeInt;
     break;
   }
   case 2: {
     type = kTypeFloat;
     break;
   }
   case 3: {
     type = kTypeChar;
     break;
   }
   default: {
     type = kTypeInvalid;
     break;
   }
  }
  uint32_t len, col_ind;
  bool nullable, unique;
  len = MACH_READ_UINT32(buf);
  ofs += sizeof(uint32_t);
  buf += sizeof(uint32_t);
  col_ind = MACH_READ_UINT32(buf);
  ofs += sizeof(uint32_t);
  buf += sizeof(uint32_t);
  nullable = MACH_READ_FROM(bool, buf);
  ofs += sizeof(bool);
  buf += sizeof(bool);
  unique = MACH_READ_FROM(bool, buf);
  ofs += sizeof(bool);
  buf += sizeof(bool);

  // can be replaced by:
  //		ALLOC_P(heap, Column)(column_name, type, col_ind, nullable, unique);
  void *mem = heap->Allocate(sizeof(Column));
  if (type == kTypeChar)
    column = new (mem) Column(column_name, type, len, col_ind, nullable, unique);
  else
    column = new (mem) Column(column_name, type, col_ind, nullable, unique);
  return ofs;
}