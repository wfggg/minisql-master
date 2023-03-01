#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

string exception_ = "out of memory";

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          root_page_id_(-1),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
  IndexRootsPage *rootrootpage=reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  ASSERT(rootrootpage!=nullptr,"rootrootpage is null!");
  page_id_t root_page_id;
  bool flag = rootrootpage->GetRootId(index_id,&root_page_id);
  if(flag==true) root_page_id_ = root_page_id;
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,false);

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  // no use
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  if(root_page_id_==INVALID_PAGE_ID){
    return true;
  }
  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  /*Page *page = FindLeafPage(key);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value;
  ASSERT(leaf_page!=nullptr,"leaf_page is null!");
  bool ret = leaf_page->Lookup(key,value,comparator_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(),false);//没有对该页进行修改，不是脏页
  result.push_back(value);
  return ret;*/
  // fetch the leaf_page
  auto leaf_page = FindLeafPage(key);
  LeafPage *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // find the value
  ValueType value_;
  auto flag = node->Lookup(key, value_, comparator_);
  // release the page
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  // store the value
  result.push_back(value_);
  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // if tree is empty
  if(IsEmpty())
  {
    StartNewTree(key, value);
    //InsertIntoLeaf(key, value);
    return true;
  }
    return InsertIntoLeaf(key, value);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  /*
  //新建一棵树，把key值插进去
  //申请一个新页
  page_id_t root_page_id;//根结点的pageid
  Page *root_page=buffer_pool_manager_->NewPage(root_page_id);
  ASSERT(root_page_id!=INVALID_PAGE_ID,"root_page_id is invalid!");
  if(root_page==nullptr){
    throw std::runtime_error("out of memory");
  }else{
    root_page_id_=root_page_id;//更新root_page_id
    //新插入的结点其实是叶结点，将其转化为叶结点并初始化
    ASSERT(root_page!=nullptr,"rootpage is null!");
    LeafPage *root_node=reinterpret_cast<LeafPage*>(root_page->GetData());
    root_node->Init(root_page_id,INVALID_PAGE_ID,leaf_max_size_);
    root_node->Insert(key,value,comparator_);
    //插入rootrootpage中
    UpdateRootPageId(1);
    buffer_pool_manager_->UnpinPage(root_page_id,true);
  }*/

  // ask for new page
  page_id_t root_page_id;
  auto new_page = buffer_pool_manager_->NewPage(root_page_id);
  // string exception_ = "out of memory";
  // if null
  if(new_page == nullptr)
  {
    throw exception_;
  }
  // if not null : update root page id
  root_page_id_= root_page_id;
  LeafPage *leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
  leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  // insert entry into leaf page
  leaf->Insert(key, value, comparator_);
  // release page
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  UpdateRootPageId(1);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  /*
  Page *page = FindLeafPage(key);

  //cout<<"already find leafpage"<<endl;
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  //调叶结点插入函数
  ASSERT(leaf_page!=nullptr,"leafpage is null!");
  auto size = leaf->GetSize();
  auto new_size = leaf->Insert(key, value, comparator_);*/

  Page *leaf_page = FindLeafPage(key);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // ValueType value_;
  // see whether insert key exist or not
  auto size = leaf->GetSize();
  auto new_size = leaf->Insert(key, value, comparator_);
  // insert key exist
  if(size == new_size)
  {
    // release not dirty
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  // need split
  if (new_size > leaf_max_size_){
    LeafPage* sibling_leaf = Split(leaf); 
    ASSERT(sibling_leaf!=nullptr,"newpage is null!");
    InsertIntoParent(leaf,sibling_leaf->KeyAt(0),sibling_leaf);
    // release dirty
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(sibling_leaf->GetPageId(),true);
    UpdateRootPageId(0);
  }
  // not need split
  else
  {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
  }
  return true;//insert success
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  // case 1: is leaf page
  // case 2: is internal page

  // allocate a new page
  ASSERT(node!=nullptr,"node is null!");
  page_id_t new_page_id;
  Page *new_page=buffer_pool_manager_->NewPage(new_page_id);
  // out of memory
  if(new_page == nullptr)
  {
    throw exception_;
    return nullptr;
  }
  // move half to new node
  else
  {
    // fetch the page
    N *new_node=reinterpret_cast<N *>(new_page->GetData());
    if(node->IsLeafPage()){
      // case 1: is leaf page
      new_node->SetPageType(IndexPageType::LEAF_PAGE);
      // change type
      LeafPage* old_leaf_node=reinterpret_cast<LeafPage*>(node);
      LeafPage* new_leaf_node=reinterpret_cast<LeafPage*>(new_node);
      // initial new node
      new_leaf_node->Init(new_page_id,old_leaf_node->GetParentPageId(),leaf_max_size_);
      // move half to
      old_leaf_node->MoveHalfTo(new_leaf_node);
      // insert page
      new_leaf_node->SetNextPageId(old_leaf_node->GetNextPageId());
      old_leaf_node->SetNextPageId(new_page_id);
  
      new_node=reinterpret_cast<N*>(new_leaf_node);
    }
    else{
      // case 2: is internal page
      new_node->SetPageType(IndexPageType::INTERNAL_PAGE);
      // change type
      InternalPage* old_internal_node=reinterpret_cast<InternalPage*>(node);
      InternalPage* new_internal_node=reinterpret_cast<InternalPage*>(new_node);
      // initial new node
      new_internal_node->Init(new_page_id,old_internal_node->GetParentPageId(),internal_max_size_);
      // move half to
      old_internal_node->MoveHalfTo(new_internal_node,buffer_pool_manager_);
      new_node=reinterpret_cast<N*>(new_internal_node);
    }
    return new_node;
  }
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
//old_node是左孩子，new_node是右孩子，key是他们的分界
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,Transaction *transaction) {
  // ASSERT(old_node!=nullptr,"oldpage is null!");
  
  // old node is root page
  if (old_node->IsRootPage()){
    // set a new root page
    page_id_t NewRootPageId = INVALID_PAGE_ID;
    Page *page = buffer_pool_manager_->NewPage(NewRootPageId);
    InternalPage*New_Root_Page = reinterpret_cast<InternalPage *>(page->GetData());
    ASSERT(new_node!=nullptr,"newpage is null!");
    // update root page id
    root_page_id_= NewRootPageId;
    // update parent page id of two nodes
    old_node->SetParentPageId(NewRootPageId);
    new_node->SetParentPageId(NewRootPageId);
    // initialize the root page
    New_Root_Page->Init(NewRootPageId,INVALID_PAGE_ID,internal_max_size_);//parent_page_id = 0 —— 代表是根节点
    // new root point to the two nodes
    New_Root_Page->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    // release 
    buffer_pool_manager_->UnpinPage(NewRootPageId,true);
  }
  // old node is not root page
  else{
    page_id_t ParentPageId = old_node->GetParentPageId();
    // fetch the parent of old node
    Page *page = buffer_pool_manager_->FetchPage(ParentPageId);
    // insert the new node after old node in parent page
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t old_node_value = old_node->GetPageId();
    page_id_t new_node_value = new_node->GetPageId();
    int new_size = parent_page->InsertNodeAfter(old_node_value,key,new_node_value);
    if (new_size <= internal_max_size_){
      // not need split
      buffer_pool_manager_->UnpinPage(ParentPageId,true);
    }
    else{
      // parent page is full : need split
      InternalPage *spilt_new_page = Split(parent_page);
      // insert into parent recursively
      KeyType new_key = spilt_new_page->KeyAt(0);
      InsertIntoParent(parent_page, new_key, spilt_new_page);
      // release dirty page
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(spilt_new_page->GetPageId(),true);
    }
  }              
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if(IsEmpty()){
    return;
  }else{
    auto leaf_page=FindLeafPage(key);
    //Page* store_leaf_page=leaf_page;
    ASSERT(leaf_page!=nullptr,"leaf_page is null");
    LeafPage* leaf_node=reinterpret_cast<LeafPage*>(leaf_page->GetData());
    
    // judge if delete success
    if(leaf_node->GetSize() > leaf_node->RemoveAndDeleteRecord(key,comparator_)){
      // release the page
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
      // judge if need delete
      bool should_delete;
      should_delete = CoalesceOrRedistribute(leaf_node,nullptr);
      if(should_delete)
      {
        buffer_pool_manager_->DeletePage(leaf_node->GetPageId());
      }
    }else{
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),false);
      return;
    }
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  /*
  //用来处理删除一条记录的后续工作，若目标页需要被删除返回true，否则返回false
  //首先需要找到当前结点的兄弟结点，如果二者合并后数目大于最大值则重新分配
  //否则都合并到兄弟结点后将当前结点删除
  if(node->IsRootPage()){
    //根结点
    //cout<<"In CoalorRedis: is root."<<endl;
    //cout<<"root page id is: "<<node->GetPageId()<<endl;
    bool ShoulDelete;
    ShoulDelete=AdjustRoot(node);
    return ShoulDelete;
  }else{
    //中间结点或叶结点
    //cout<<"In CoalorRedis: isn't root."<<endl;
    if(node->GetSize()>=node->GetMinSize()){
      //不需要调整
      return false;
    }else{
      //需要判断是否为第一个结点，是不是第一个会影响兄弟结点的下标
      Page* parent_page=buffer_pool_manager_->FetchPage(node->GetParentPageId());
      ASSERT(parent_page!=nullptr,"parent_page is null!");
      InternalPage* parent_node=reinterpret_cast<InternalPage*>(parent_page->GetData());
      //获得当前结点的下标,value的值其实就是page_id
      int node_index=parent_node->ValueIndex(node->GetPageId());
      int sibling_index;
      if(node_index==0){
        //第一个孩子
        //cout<<"is the first child"<<endl;
        sibling_index=1;
      }else{
        sibling_index=node_index-1;
      }
      //寻找兄弟结点
      page_id_t sibling_page_id=parent_node->ValueAt(sibling_index);
      Page* sibling_page=buffer_pool_manager_->FetchPage(sibling_page_id);
      ASSERT(sibling_page!=nullptr,"sibling page is null!");
      N* sibling_node=reinterpret_cast<N*>(sibling_page->GetData());
      //判断要执行的操作是重新分配还是合并
      //约定大于Maxsize才分裂
      //大于等于Maxsize的时候可以通过重分配来解决
      
      if(sibling_node->GetSize()+node->GetSize()>=node->GetMaxSize()){
        //重新分配sibling和当前
        //cout<<"use redistrubute"<<endl;
        //传的index是当前结点的index
        Redistribute(sibling_node,node,node_index);
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
        buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(),true);
        return false;//不需要删除
      }else{
        //cout<<"use coalesce"<<endl;
        //cout<<"Parent id1: "<<parent_node->GetPageId()<<endl;
        bool ParentShouldDelete;
        ParentShouldDelete=Coalesce(&sibling_node,&node,&parent_node,node_index,nullptr);
        
        ////////////////
        if(ParentShouldDelete==true){
          //cout<<"should delete parent"<<endl;
          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
          //cout<<"parent_page_id : "<<parent_page->GetPageId()<<endl;
          //cout<<"parent_node_id : "<<parent_node->GetPageId()<<endl;
          buffer_pool_manager_->DeletePage(parent_node->GetPageId());
          
          ////////////////
        }
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
        buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(),true);
        return true;//该结点需要被删除
      }
    }
  }*/
  // if is root
  if(node->IsRootPage())
  {
    auto flag = AdjustRoot(node);
    return flag;
  }
  // if not root
  if(node->GetSize() >= node->GetMinSize())
  {
    // no deletion happens
    return false;
  }
  // first: find the sibling of input page
  // auto input_page = reinterpret_cast<BPlusTreePage *>(node->GetData());
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage* parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  auto index = parent_node->ValueIndex(node->GetPageId());
  
  auto idx = ( index == 0 ? 1 : (index - 1) );
  auto sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(idx));
  //sibling_page->WLatch();
  N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  // second： decide case
  // case 1: redistribute
  if(node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize())
  {
    Redistribute(sibling_node, node, index);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    //sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return false;
  }
  // case 2: coalesce
  Coalesce(&sibling_node, &node, &parent_node, index, transaction);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  //sibling_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
//合并两个结点 index是node的下标
//大多数情况，neighbor_node在node的前面 （neighbour_node,node)
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  bool is_leaf = (*node)->IsLeafPage();
  if (index == 0){//(node,neighbor_node)
    //cout<<"In Coalesce: is first child"<<endl;
    if (is_leaf){
      // is leaf page
      Page* first_page=FindLeafPage((*node)->array_[1].first,true);
      
      LeafPage* first_node=reinterpret_cast<LeafPage*>(first_page->GetData());
      if ((*node)->GetPageId()!=first_node->GetPageId()){
        while(1){
          if(first_node->GetNextPageId()==(*node)->GetPageId()){
            first_node->SetNextPageId((*neighbor_node)->GetPageId());
            break;
          }
          page_id_t next_page_id=first_node->GetNextPageId();
          Page* next_page=buffer_pool_manager_->FetchPage(next_page_id);
          LeafPage* next_node=reinterpret_cast<LeafPage*>(next_page->GetData());
          buffer_pool_manager_->UnpinPage(next_page_id,false);
          first_node=next_node;
        }
      }
      
      //////////////////
      LeafPage *BefLeaf = reinterpret_cast<LeafPage *>(*node);
      LeafPage *RearLeaf = reinterpret_cast<LeafPage *>(*neighbor_node);
      BefLeaf->MoveAllToFrontOf(RearLeaf);
      
    }
    else{
      // is internal page
      InternalPage *BefPage = reinterpret_cast<InternalPage *>(*node);
      InternalPage *RearPage = reinterpret_cast<InternalPage *>(*neighbor_node);
      
      BefPage->MoveAllToFrontOf(RearPage,(*parent)->KeyAt(1),buffer_pool_manager_);
      
    }
    //index++;
    (*parent)->array_[1].first = (*parent)->array_[0].first;
  }
  // not first node
  else{
    if (is_leaf){
      // is leaf page
      LeafPage *BefLeaf = reinterpret_cast<LeafPage *>(*neighbor_node);
      LeafPage *RearLeaf = reinterpret_cast<LeafPage *>(*node);
      RearLeaf->MoveAllTo(BefLeaf);
    }
    else{
      // is internal page
      InternalPage *BefPage = reinterpret_cast<InternalPage *>(*neighbor_node);
      InternalPage *RearPage = reinterpret_cast<InternalPage *>(*node);
      RearPage->MoveAllTo(BefPage,(*parent)->KeyAt(index),buffer_pool_manager_);
    }
  }
  (*parent)->Remove(index);
  
  return CoalesceOrRedistribute(*parent,transaction);
}


/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
//重新分配——从兄弟那儿借一个
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  /*
  bool is_leaf = node->IsLeafPage();
  if (is_leaf){
    LeafPage* neighbor_leaf = reinterpret_cast<LeafPage *>(neighbor_node);
    LeafPage* leaf = reinterpret_cast<LeafPage *>(node);
    //由于从兄弟借之后，会影响父节点的内容
    //需要取父节点
    ASSERT(neighbor_leaf!=nullptr,"In Redistribute: neighbor leaf is null!");
    ASSERT(leaf!=nullptr,"In Redistribute: leaf is null!");
    page_id_t parent_page_id = leaf->GetParentPageId();
    Page* page = buffer_pool_manager_->FetchPage(parent_page_id);
    InternalPage* parent_page = reinterpret_cast<InternalPage *>(page->GetData());
    ASSERT(parent_page!=nullptr,"In Redistribute: parent_page is null!");
    if (index == 0){
      //这时neighbor_node在node的后面
      //这时候(node,neighbor_node)
      //把neighbor_node的第一对key&value放到node的最后
      neighbor_leaf->MoveFirstToEndOf(leaf);
      //父节点的第一个孩子是node，第二个孩子是neighbor_node
      //       第一个key 是非法，第二个key 是neighbor_node中最小的
      parent_page->SetKeyAt(1,neighbor_leaf->KeyAt(0));
      //unpin父页并设为脏页
      buffer_pool_manager_->UnpinPage(parent_page_id,true);
    }
    else{
      //cout<<"In Redistribute: not the first node"<<endl;
      //这时候(neighbor_node,node)
      //把neighbor_node的最后一对key&value放到node的前面
      ASSERT(neighbor_leaf!=nullptr,"In Redistribute 1: neighbor leaf is null!");
      ASSERT(leaf!=nullptr,"In Redistribute 1: leaf is null!");
      neighbor_leaf->MoveLastToFrontOf(leaf);
      //父节点的第index-1个孩子是neighbor_node，第index个孩子是node
      //       第index-1个key    不变        ，第index个key 是node中最小的
      ASSERT(parent_page!=nullptr,"In Redistribute 1: parent_page is null!");
      //cout<<"In Redistribute: index is"<<index<<endl;
      parent_page->SetKeyAt(index,leaf->KeyAt(0));
      //unpin父页并设为脏页
      buffer_pool_manager_->UnpinPage(parent_page_id,true);
    }
  }
  else{
    InternalPage* neighbor_internal = reinterpret_cast<InternalPage *>(neighbor_node);
    InternalPage* internal = reinterpret_cast<InternalPage *>(node);
    //由于从兄弟借之后，会影响父节点的内容
    //需要取父节点
    page_id_t parent_page_id = internal->GetParentPageId();
    Page* page = buffer_pool_manager_->FetchPage(parent_page_id);
    InternalPage* parent_page = reinterpret_cast<InternalPage *>(page->GetData());
    if (index == 0){
      //这时候(node,neighbor_node)
      //把neighbor_node的第一对key&value放到node的最后
      //注意neighbor_node的第一对中的key是非法的，故需要从parent中获得
      KeyType middle_key = parent_page->KeyAt(1);
      //KeyType middle_key=internal->KeyAt(0);
      neighbor_internal->MoveFirstToEndOf(internal,middle_key,buffer_pool_manager_);
      //父节点的第一个孩子是node，第二个孩子是neighbor_node
      //       第一个key 是非法，第二个key 是neighbor_node中最小的
      parent_page->SetKeyAt(1,neighbor_internal->KeyAt(0));
      //unpin父页并设为脏页
      buffer_pool_manager_->UnpinPage(parent_page_id,true);
    }
    else{
      //这时候(neighbor_node,node)
      //把neighbor_node的最后一对key&value放到node的前面
      //KeyType middle_key = parent_page->KeyAt(index);
      KeyType middle_key=neighbor_internal->KeyAt(neighbor_internal->GetSize()-1);
      //cout<<"middle key: "<<middle_key<<endl;
      neighbor_internal->MoveLastToFrontOf(internal,middle_key,buffer_pool_manager_);
      //父节点的第index-1个孩子是neighbor_node，第index个孩子是node
      //       第index-1个key    不变        ，第index个key 是node中最小的
      parent_page->SetKeyAt(index,node->KeyAt(0));
      //unpin父页并设为脏页
      buffer_pool_manager_->UnpinPage(parent_page_id,true);
    }
  }*/
    index_id_t dest_index = index;
  // fetch the parent page
  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(page->GetData());
  // case 1: if node is leaf
  if(node->IsLeafPage())
  {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if(index == 0)
    {
      dest_index = 1;
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
      parent->SetKeyAt(dest_index, neighbor_leaf_node->KeyAt(0));
    }
    else
    {
      //dest_index = index;
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      parent->SetKeyAt(dest_index, leaf_node->KeyAt(0));
    }
  }
  // case 2: if node is internal 
  else
  {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if(index == 0)
    {
      dest_index = 1;
      neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(dest_index), buffer_pool_manager_);
      parent->SetKeyAt(dest_index, neighbor_internal_node->KeyAt(0));
    }
    else
    {
      //dest_index = index;
      neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(dest_index), buffer_pool_manager_);
      parent->SetKeyAt(dest_index, internal_node->KeyAt(0));
    }
  }
  
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  /*//如果有必要的话，更新根结点
  //根结点的size可以小于minsize
  //可能是删掉了root中最后一个元素但是root还有孩子，也可能是整棵树都空了
  //根结点需要删除则返回true，否则返回false
  //在case1的情况下，root是内部结点；case2的情况下，root是叶结点
  if(old_root_node->IsLeafPage() && old_root_node->GetSize()==0){
    //case2 删除这棵树
    //cout<<"case 2"<<endl;
    root_page_id_=INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }else if(!old_root_node->IsLeafPage() && old_root_node->GetSize()==1){
    //case1 让孩子成为新的root
    //cout<<"case 1"<<endl;
    InternalPage* old_root=reinterpret_cast<InternalPage*>(old_root_node);
    page_id_t child_page_id;
    child_page_id=old_root->RemoveAndReturnOnlyChild();
    //更新整棵树的root_page_id
    root_page_id_=child_page_id;
    UpdateRootPageId(0);
    //更新新根结点的parent_page_id
    Page* new_root_page=buffer_pool_manager_->FetchPage(child_page_id);
    InternalPage* new_root_node=reinterpret_cast<InternalPage*>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(child_page_id,true);
    return true;
  }else{
    //cout<<"case 3"<<endl;
    //不需要删除根结点
    return false;
  }*/
  // case 1: when delete the last element in root page 
  //         but root page still has one last child
  if(!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1)
  {
    // set the only child into root
    InternalPage *root = reinterpret_cast<InternalPage *>(old_root_node);
    auto child_page = buffer_pool_manager_->FetchPage(root->ValueAt(0));
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page);
    child_node->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = child_node->GetPageId();
    // update root page id
    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
    return true;
  }
  // case 2: when delete the last element in whole b+ tree

  // old root node is leaf and size = 0
  if(old_root_node->IsLeafPage() && old_root_node->GetSize() == 0)
    return true;
  else
    return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  Page* first_page=FindLeafPage(KeyType(),true);
  LeafPage* first_node=reinterpret_cast<LeafPage*>(first_page->GetData());
  return INDEXITERATOR_TYPE(first_node,0,buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *page = FindLeafPage(key);
  LeafPage *leaf_page =  reinterpret_cast<LeafPage *>(page->GetData());
  int index = leaf_page->KeyIndex(key,comparator_);
  return INDEXITERATOR_TYPE(leaf_page,index,buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  Page *page = FindLeafPage(KeyType(),false,true);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  return INDEXITERATOR_TYPE(leaf_page,leaf_page->GetSize(),buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */


INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost,bool rightMost) {
  // fetch root page
  Page* page = buffer_pool_manager_->FetchPage(root_page_id_);

  buffer_pool_manager_->UnpinPage(root_page_id_,false);
  BPlusTreePage* node =reinterpret_cast<BPlusTreePage*>(page->GetData());
  // loop to find the leaf
  while(!node->IsLeafPage()){
    
    InternalPage* internal_node=reinterpret_cast<InternalPage*>(node);
    // find the children
    page_id_t child_page_id;
    if(leftMost == true)
    {
      child_page_id=internal_node->ValueAt(0);
    }
    else if(rightMost == true)
    {
      child_page_id=internal_node->ValueAt(internal_node->GetSize()-1);
    }
    else
    {
      child_page_id = internal_node->Lookup(key,comparator_);
    }
    // fetch child page
    Page* child_page = buffer_pool_manager_->FetchPage(child_page_id);
    ASSERT(child_page != nullptr,"child page is null!");
    BPlusTreePage* child_node = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
    buffer_pool_manager_->UnpinPage(child_page_id,false);
    // 
    page = child_page;
    node = child_node;
  }
  return page;
}


/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  IndexRootsPage *root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if(insert_record != 0)
  {
    // insert a recond <index_name, root_page_id> into header page 
    root_page->Insert(index_id_, INDEX_ROOTS_PAGE_ID);
  }
  else
  {
    root_page->Update(index_id_, INDEX_ROOTS_PAGE_ID);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}
/**
 * This method is used for debug only, You don't need to modify
 */


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}





template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;