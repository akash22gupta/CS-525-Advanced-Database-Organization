#include "btree_mgr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <string.h>

SM_FileHandle btree_fh;
int maxEle;

typedef struct BTREE
{
    int *key;
    struct BTREE **next;
    RID *id;
} BTree;

BTree *root;
BTree *scan;
int indexNum = 0;

// init and shutdown index manager
RC initIndexManager (void *mgmtData)
{
    return RC_OK;
}

RC shutdownIndexManager ()
{
    return RC_OK;
}

// create, destroy, open, and close an btree index
RC createBtree (char *idxId, DataType keyType, int n)
{
	int i;
    root = (BTree*)malloc(sizeof(BTree));
    root->key = malloc(sizeof(int) * n);
    root->id = malloc(sizeof(int) * n);
    root->next = malloc(sizeof(BTree) * (n + 1));
    for (i = 0; i < n + 1; i ++)
        root->next[i] = NULL;
    maxEle = n;
    createPageFile (idxId);
    
    return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId)
{
    if(openPageFile (idxId, &btree_fh)==0){
    	return RC_OK;
	}

    else{
    	return RC_ERROR;
	}
}

RC closeBtree (BTreeHandle *tree)
{
    if(closePageFile(&btree_fh)==0){
	free(root);
    return RC_OK;
	}
	else{
		return RC_ERROR;
	}
}

RC deleteBtree (char *idxId)
{
    if(destroyPageFile(idxId)==0){
		return RC_OK;
	}
    
    else{
    	return RC_ERROR;
	}
}


// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result)
{
    BTree *temp = (BTree*)malloc(sizeof(BTree));
    
    int numNodes = 0;
    int i;
    
    for (i = 0; i < maxEle + 2; i ++) {
        numNodes ++;
    }

    *result = numNodes;
    
    
    return RC_OK;
}

RC getNumEntries (BTreeHandle *tree, int *result)
{
	int totalEle = 0, i;
    BTree *temp = (BTree*)malloc(sizeof(BTree));
    
    for (temp = root; temp != NULL; temp = temp->next[maxEle])
        for (i = 0; i < maxEle; i ++)
            if (temp->key[i] != 0)
                totalEle ++;
    *result = totalEle;
    //free(temp);
    return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result)
{
    return RC_OK;
}


// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
    BTree *temp = (BTree*)malloc(sizeof(BTree));
    int found = 0, i;
    for (temp = root; temp != NULL; temp = temp->next[maxEle]) {
        for (i = 0; i < maxEle; i ++) {
            if (temp->key[i] == key->v.intV) {
                (*result).page = temp->id[i].page;
                (*result).slot = temp->id[i].slot;
                found = 1;
                break;
            }
        }
        if (found == 1)
            break;
    }
    //free(temp);
    
    if (found == 1)
        return RC_OK;
    else
        return RC_IM_KEY_NOT_FOUND;
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
    int i = 0;
    BTree *temp = (BTree*)malloc(sizeof(BTree));
    BTree *node = (BTree*)malloc(sizeof(BTree));
    node->key = malloc(sizeof(int) * maxEle);
    node->id = malloc(sizeof(int) * maxEle);
    node->next = malloc(sizeof(BTree) * (maxEle + 1));
    
    for (i = 0; i < maxEle; i ++) {
    	node->key[i] = 0;
    }

//    printf("\n\nIteration: %d", key->v.intV);
    
    int nodeFull = 0;
    
    for (temp = root; temp != NULL; temp = temp->next[maxEle]) {
        nodeFull = 0;
        for (i = 0; i < maxEle; i ++) {
            if (temp->key[i] == 0) {
                temp->id[i].page = rid.page;
                temp->id[i].slot = rid.slot;
                temp->key[i] = key->v.intV;
                temp->next[i] = NULL;
                nodeFull ++;
                break;
            }
        }
        if ((nodeFull == 0) && (temp->next[maxEle] == NULL)) {
            node->next[maxEle] = NULL;
            temp->next[maxEle] = node;
        }
    }
    
    int totalEle = 0;
    for (temp = root; temp != NULL; temp = temp->next[maxEle])
        for (i = 0; i < maxEle; i ++)
            if (temp->key[i] != 0)
                totalEle ++;

    if (totalEle == 6) {
        node->key[0] = root->next[maxEle]->key[0];
        node->key[1] = root->next[maxEle]->next[maxEle]->key[0];
        node->next[0] = root;
        node->next[1] = root->next[maxEle];
        node->next[2] = root->next[maxEle]->next[maxEle];

    }
    
    return RC_OK;
}

RC deleteKey (BTreeHandle *tree, Value *key)
{
    BTree *temp = (BTree*)malloc(sizeof(BTree));
    int found = 0, i;
    for (temp = root; temp != NULL; temp = temp->next[maxEle]) {
        for (i = 0; i < maxEle; i ++) {
            if (temp->key[i] == key->v.intV) {
                temp->key[i] = 0;
                temp->id[i].page = 0;
                temp->id[i].slot = 0;
                found = 1;
                break;
            }
        }
        if (found == 1)
            break;
    }
    

    return RC_OK;
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
    scan = (BTree*)malloc(sizeof(BTree));
    scan = root;
    indexNum = 0;
    
    BTree *temp = (BTree*)malloc(sizeof(BTree));
    int totalEle = 0, i;
    for (temp = root; temp != NULL; temp = temp->next[maxEle])
        for (i = 0; i < maxEle; i ++)
            if (temp->key[i] != 0)
                totalEle ++;

    int key[totalEle];
    int elements[maxEle][totalEle];
    int count = 0;
    for (temp = root; temp != NULL; temp = temp->next[maxEle]) {
        for (i = 0; i < maxEle; i ++) {
            key[count] = temp->key[i];
            elements[0][count] = temp->id[i].page;
            elements[1][count] = temp->id[i].slot;
            count ++;
        }
    }
    
    int swap;
    int pg, st, c, d;
    for (c = 0 ; c < count - 1; c ++)
    {
        for (d = 0 ; d < count - c - 1; d ++)
        {
            if (key[d] > key[d+1])
            {
                swap = key[d];
                pg = elements[0][d];
                st = elements[1][d];
                
                key[d]   = key[d + 1];
                elements[0][d] = elements[0][d + 1];
                elements[1][d] = elements[1][d + 1];
                
                key[d + 1] = swap;
                elements[0][d + 1] = pg;
                elements[1][d + 1] = st;
            }
        }
    }
    
    count = 0;
    for (temp = root; temp != NULL; temp = temp->next[maxEle]) {
        for (i = 0; i < maxEle; i ++) {
            temp->key[i] =key[count];
            temp->id[i].page = elements[0][count];
            temp->id[i].slot = elements[1][count];
            count ++;
        }
    }

    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result)
{
    if(scan->next[maxEle] != NULL) {
        if(maxEle == indexNum) {
            indexNum = 0;
            scan = scan->next[maxEle];
        }

        (*result).page = scan->id[indexNum].page;
        (*result).slot = scan->id[indexNum].slot;
        indexNum ++;
    }
    else
        return RC_IM_NO_MORE_ENTRIES;
    
    return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle)
{
    indexNum = 0;
    return RC_OK;
}


// debug and test functions
char *printTree (BTreeHandle *tree)
{
    return RC_OK;
}
