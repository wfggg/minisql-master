#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t move=0;  //the offset based on buf
  //store the magic num into buffer
  MACH_WRITE_TO(uint32_t,buf,TABLE_METADATA_MAGIC_NUM);
  uint32_t len_magic=sizeof(uint32_t);
  move+=len_magic;
  //store table_id into buffer
  MACH_WRITE_TO(table_id_t,buf+move,table_id_);
  uint32_t len_table_id=sizeof(table_id_t);
  move+=len_table_id;
  //store name into buffer
  uint32_t len_name=table_name_.size();
  MACH_WRITE_TO(uint32_t,buf+move,len_name);
  move+=sizeof(uint32_t);
  MACH_WRITE_STRING(buf+move,table_name_);
  move+=len_name*sizeof(char);
  //store root_page_id into buffer
  MACH_WRITE_TO(page_id_t,buf+move,root_page_id_);
  uint32_t len_page_id=sizeof(page_id_t);
  move+=len_page_id;
  //store magic_num of schema_ into buffer
  schema_->SerializeTo(buf+move);
  /*MACH_WRITE_TO(uint32_t,buf+move,schema->SCHEMA_MAGIC_NUM);
  move+=len_magic;*/
  //store columns of schema_ into buffer
  /*uint32_t size_column = schema->columns_.size();
  uint32_t len_column=size_column * sizeof(Column*);
  MACH_WRITE_TO(uint32_t,buf+move,len_column);
  move+=sizeof(uint32_t);
  std::vector<Column *>::iterator it;
  //store the elements of key_map_ into buffer
  for(it=schema->columns_.begin();it!=schema->columns_.end();it++)
  {
    MACH_WRITE_TO(Column *,buf+move,(*it));
    move+=sizeof(Column *);
  }*/
  return 0;
}

uint32_t TableMetadata::GetSerializedSize() const {
  uint32_t sum=0;
  //add magic and table_id
  sum+=sizeof(uint32_t);
  sum+=sizeof(table_id_t);
  uint32_t length=table_name_.size();
  sum+=length*sizeof(char);
  sum+=sizeof(page_id_t);
  sum += schema_->GetSerializedSize();
  //schema *
  return sum;
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  uint32_t move=0;
  //read magic_num
  uint32_t val=MACH_READ_FROM(uint32_t,buf);
  if(val!=344528)
  {
    return 0;
  }
  move+=sizeof(uint32_t);
  //read table_id_
  uint32_t table_id=MACH_READ_FROM(table_id_t,buf+move);
  move+=sizeof(table_id_t);
  //read table_name_
  uint32_t len_name=MACH_READ_FROM(uint32_t,buf+move);
  move+=sizeof(uint32_t);
  uint32_t size_name=len_name/sizeof(char);
  string table_name;
  for(uint32_t i=0;i<size_name;i++)
  {
    table_name.append(1,MACH_READ_FROM(char,buf+move));
    move+=sizeof(char);
  }
  /*table_meta->table_name_=MACH_READ_FROM(std::string,buf+move);
  move+=len_name;*/
  //read root_page_id_
  page_id_t root_page_id=MACH_READ_FROM(page_id_t,buf+move);
  move+=sizeof(page_id_t);
  //read schema
  //table_meta->schema_=(Schema *)malloc(sizeof(Schema));
  Schema *schema;
  Schema::DeserializeFrom(buf+move,schema,heap);

  table_meta=TableMetadata::Create(table_id,table_name,root_page_id,schema,heap);
  //read magic_num of schema
  /*table_meta->schema->SCHEMA_MAGIC_NUM = MACH_READ_FROM(uint32_t,buf+move);
  move+=sizeof(uint32_t);*/
  //read column of schema
  /*uint32_t len_column=MACH_READ_FROM(uint32_t,buf+move);
  move+=sizeof(uint32_t);
  uint32_t size_column=len_column/sizeof(Column *);
  for(int i=0;i<size_column;i++)
  {
    table_meta->schema->columns_.push_back(MACH_READ_FROM(Column *,buf+move));
    move+=sizeof(Column *);
  }*/
  return 0;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
