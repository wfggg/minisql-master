#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 10;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
    //tree.Insert(i, i);
    tree.PrintTree(mgr[i]);
    cout << "kes[" << i << "] = " << keys[i] << "values[" << i << "]m = " << values[i] << endl;
  }
  cout << "printtree start" << endl;
  tree.PrintTree(mgr[10]);
  cout << "printtree end" << endl;                              
  //ASSERT_TRUE(tree.Check());
  cout << "check unpin start" << endl;
  // Print tree
  // tree.PrintTree(mgr[0]);
  cout << "check unpin end" << endl;
  
  // Check valid
  cout << "print tree after delete end" << endl;
  
  // Search keys
  
  vector<int> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(i, ans);
    //cout << kv_map[i] << " " << ans[i] << endl;
    ASSERT_EQ(kv_map[i], ans[i]);
    //cout << kv_map[i] << " " << ans[i] << endl;
  }
  //ASSERT_TRUE(tree.Check());
  
  // Delete half keys
  for (int i = 0; i < n / 2; i++) {
    tree.Remove(delete_seq[i]);
  }
  tree.PrintTree(mgr[11]);
  // Check valid
  
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
  
}