#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  uint32_t ofs=0;
  int len = fields_.size();
  for (int i = 0; i < len; i++) {
    auto *tmp = this->fields_[i];  //取出row中的每个Field指
    // std::cout<<i<<std::endl;
    tmp->SerializeTo(buf);
    buf = buf + tmp->GetSerializedSize();
    ofs = ofs + tmp->GetSerializedSize();
  }
  return ofs;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  // replace with your code here

  uint32_t len = schema->GetColumnCount();
  MemHeap *heap = new SimpleMemHeap();
  uint32_t ofs = 0;
  for (int i = 0; i < int(len); i++) {
    Field *f;
    uint32_t ofss;
     ofss= f->Field::DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &f, false, heap);
     ofs=ofs+ofss;
     buf=buf+ofss;
    fields_.push_back(f);
  }
  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here
  uint32_t ofs = 0;
  int len = fields_.size();
  for (int i = 0; i < len; i++) {
    auto tmp = this->fields_[i];  //取出row中的每个Field指针
    ofs += tmp->GetSerializedSize();
  }
  return ofs;
 }