#include "btree_mgr.h"
#include <stdlib.h>
//#include "dberror.h"

// init and shutdown index manager
RC
initIndexManager (void *mgmtData)
{
  return RC_OK;
}

RC
shutdownIndexManager ()
{
  return RC_OK;
}


// create, destroy, open, and close an btree index
RC
createBtree (char *idxId, DataType keyType, int n)
{
  return RC_OK;
}

RC
openBtree (BTreeHandle **tree, char *idxId)
{
  BTreeHandle *result = (BTreeHandle *) malloc(sizeof(BTreeHandle));

  result->keyType = DT_INT;
  result->idxId = idxId;
  result->mgmtData = NULL;

  *tree = result;
  return RC_OK;
}

RC
closeBtree (BTreeHandle *tree)
{
  return RC_OK;
}

RC
deleteBtree (char *idxId)
{
  return RC_OK;
}


// access information about a b-tree
RC
getNumNodes (BTreeHandle *tree, int *result)
{
  return RC_OK;
}

RC
getNumEntries (BTreeHandle *tree, int *result)
{
  *result = -1;
  return RC_OK;
}

RC
getKeyType (BTreeHandle *tree, DataType *result)
{
  *result = DT_INT;
  return RC_OK;
}


// index access
RC
findKey (BTreeHandle *tree, Value *key, RID *result)
{
  return RC_IM_KEY_NOT_FOUND;
}

RC
insertKey (BTreeHandle *tree, Value *key, RID rid)
{
  return RC_OK;
}

RC
deleteKey (BTreeHandle *tree, Value *key)
{
  return RC_OK;
}

RC
openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
  BT_ScanHandle *result = (BT_ScanHandle *) malloc(sizeof(BT_ScanHandle));

  result->tree = tree;
  result->mgmtData = NULL;

  return RC_OK;
}

RC
nextEntry (BT_ScanHandle *handle, RID *result)
{
  return RC_IM_NO_MORE_ENTRIES;
}

RC
closeTreeScan (BT_ScanHandle *handle)
{
  free(handle);
  return RC_OK;
}


// debug and test functions
char *
printTree (BTreeHandle *tree)
{
  return "";
}
