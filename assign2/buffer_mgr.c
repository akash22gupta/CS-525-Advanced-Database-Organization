#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define RC_QUEUE_IS_EMPTY 5;
#define RC_NO_FREE_BUFFER_ERROR 6;

SM_FileHandle *fh;
Queue *q;
//pageInfo *bufferInfo[80];
int readIO;
int writeIO;
//pageInfo *bf;

RC pinPageLRU(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum);
RC pinPageFIFO(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum);


void createQueue(BM_BufferPool *const bm){
	pageInfo *newPage[bm->numPages];
	int lastPage = (bm->numPages) - 1;
	int k;
	for (k = 0; k <= lastPage; k++) {
		newPage[k] = (pageInfo*) malloc(sizeof(pageInfo));
	}
	for (k = 0; k <= lastPage; k++) {
		newPage[k]->frameNum = k;
		newPage[k]->isDirty = 0;
		newPage[k]->fixCount = 0;
		newPage[k]->pageNum = -1;
		newPage[k]->bufferData = bm->mgmtData;
	}
	int i;
	for (i = 0; i <= lastPage; i++) {
		int k = i;
		if (k == 0)
		{
			newPage[k]->prevPageInfo = NULL;
			newPage[k]->nextPageInfo = newPage[k + 1];
		}

		else if (k == lastPage) {
			newPage[k]->nextPageInfo = NULL;
			newPage[k]->prevPageInfo = newPage[k - 1];
		}
		else {

			newPage[k]->nextPageInfo = newPage[k + 1];
			newPage[k]->prevPageInfo = newPage[k - 1];
		}
	}
	q->head = newPage[0];
	q->tail = newPage[lastPage];
	q->filledframes = 0;
	q->totalNumOfFrames = bm->numPages;
}

RC emptyQueue() {
	return (q->filledframes == 0);
}

pageInfo* createNewList(int pageNumber) {
	pageInfo* newpinfo = (pageInfo*) malloc(sizeof(pageInfo));

	newpinfo->nextPageInfo = NULL;
	newpinfo->pageNum = pageNumber;
	newpinfo->isDirty = 0;
	newpinfo->fixCount = 1;

	return newpinfo;
}

pageInfo* searchQueue(BM_PageHandle * const page) {
	pageInfo *pinfo;
	pinfo = q->head;
	int i;
   // printf("\n q->filledframes %d  \n ",q->filledframes );
    //printf("\n q->totalNumOfFrames  %d \n ",q->totalNumOfFrames );

	for ( i = 0; i < q->filledframes; i++)
    {
 //printf("\n pinfo->frameNum %d  \n  ",pinfo->frameNum );
 //printf("\n page->pageNum %d line4 \n ",page->pageNum );
		if (pinfo->pageNum == page->pageNum)
			return pinfo;
		else {
			pinfo = pinfo->nextPageInfo;
 //printf("\n pinfo->frameNum %d line5 \n ",pinfo->frameNum );
 //printf("\n page->pageNum %d line6 \n ",page->pageNum );

		}
	}
//printf("line 7");
	return NULL;
}

RC deQueue() {
	if (emptyQueue()) {
		return RC_QUEUE_IS_EMPTY;
	}


	int tail_pagenum;
	pageInfo *pinfo = q->tail;

	for (int i = 0; i < q->totalNumOfFrames; i++) {

		if ((pinfo->fixCount) == 0) {

			if (pinfo->pageNum == q->tail->pageNum) {

				q->tail = (q->tail->prevPageInfo);
				q->tail->nextPageInfo = NULL;

			} else {

				pinfo->prevPageInfo->nextPageInfo = pinfo->nextPageInfo;
				pinfo->nextPageInfo->prevPageInfo = pinfo->prevPageInfo;
			}

		} else {
			tail_pagenum=pinfo->pageNum;
			//printf("\n next node%d", pinfo->pageNum);
			pinfo = pinfo->nextPageInfo;

		}
	}

	if (tail_pagenum == q->tail->pageNum){
		//printf("\nBuffer not free");
		return 0;		//Add error

	}

	if (pinfo->isDirty == 1) {
		writeBlock(pinfo->pageNum, fh, pinfo->bufferData);
		//printf("write block");
		writeIO++;
	}

	q->filledframes--;

	return RC_OK;
}

RC Enqueue(BM_PageHandle * const page, const PageNumber pageNum,BM_BufferPool * const bm) {

	if (q->filledframes == q->totalNumOfFrames ) { //If frames are full remove a page
		deQueue();
	}

	pageInfo* pinfo = createNewList(pageNum);
	pinfo->bufferData=bm->mgmtData;

	if (emptyQueue()) {
		//printf("Empty queue");
		pinfo->frameNum = q->head->frameNum;
		pinfo = q->head;
		pinfo->pageNum = pageNum;
		pinfo->fixCount=0;
	//	pinfo->bufferData=q->head->bufferData;
		q->head = pinfo;
		//q->tail=pinfo;
		pageInfo *p = q->head;
		for (int i = 0; i <= q->filledframes; i++) {
			if (i == (q->filledframes)) {
				q->tail = p;
			} else
				p = p->nextPageInfo;

		}

		//pinfo.bufferDATA 				//add buffer data

	} else {
		//printf("\nread block");
		readBlock(pageNum, fh, pinfo->bufferData);
		page->data = pinfo->bufferData;
		readIO++;
		pinfo->nextPageInfo = q->head;
		q->head->prevPageInfo = pinfo;
		q->head = pinfo;

		pageInfo *p = q->head;
		for (int i = 0; i <= q->filledframes; i++) {
			if (i == (q->filledframes)) {
				q->tail = p;
			} else
				p = p->nextPageInfo;

		}

	}
	q->filledframes++;

	return RC_OK;
}

	RC pinPageLRU(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) {

	int pageFound = 0;
	pageInfo *pinfo = q->head;
	int i;
	for ( i= 0; i < bm->numPages; i++) {
		if (pageFound == 0) {
			if (pinfo->pageNum == pageNum) {
				pageFound = 1;
				break;
			}
			pinfo = pinfo->nextPageInfo;
		}
	}

	if (pageFound == 0)
		Enqueue(page,pageNum,bm);

	if (pageFound == 1) {
		pinfo->fixCount++;
		page->data = pinfo->bufferData;

		if (pinfo == q->head) {
			pinfo->nextPageInfo = q->head;
			q->head->prevPageInfo = pinfo;
			q->head = pinfo;
		}

		if (pinfo != q->head) {
			pinfo->prevPageInfo->nextPageInfo = pinfo->nextPageInfo;
			if (pinfo->nextPageInfo) {
				pinfo->nextPageInfo->prevPageInfo = pinfo->prevPageInfo;

				if (pinfo == q->tail) {
					q->tail = pinfo->prevPageInfo;
					q->tail->nextPageInfo = NULL;
				}
				pinfo->nextPageInfo = q->head;
				pinfo->prevPageInfo = NULL;
				pinfo->nextPageInfo->prevPageInfo = pinfo;
				q->head = pinfo;
			}
		}
	}
	pageInfo *p = q->head;
	for (int i = 0; i < q->totalNumOfFrames; i++) {
		p->frameNum = i;
		p = p->nextPageInfo;
	}
	return RC_OK;
}

RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum){
	int res;
	if(bm->strategy==RS_FIFO)
		res=pinPageFIFO(bm,page,pageNum);
	else
		res=pinPageLRU(bm,page,pageNum);
	return res;
}


void updateBM_BufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy){

	 char* buffersize = (char *)calloc(PAGE_SIZE,sizeof(char));

	   bm->pageFile = (char *)pageFileName;
	   bm->numPages = numPages;
	   bm->strategy = strategy;
	   bm->mgmtData = buffersize;
	//free(buffersize);
}

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
       //  printf("Here 0");
       	    readIO = 0;
	    writeIO = 0;
	   fh = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
	   q = (Queue *)malloc(sizeof(Queue));

	  updateBM_BufferPool(bm,pageFileName,numPages,strategy);

	  openPageFile(bm->pageFile,fh);

	  createQueue(bm);

	//free(q);
	   return RC_OK;

}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
	int i, returncode = -1;
	pageInfo *pinfo=NULL,*temp=NULL;

	pinfo=q->head;

	for(i=0; i< q->totalNumOfFrames; i++)
	{
		if((pinfo->fixCount)==0 && pinfo->isDirty == 1)
		{
			writeBlock(pinfo->pageNum,fh,pinfo->bufferData);
			writeIO++;
			pinfo->isDirty=0;
			pinfo=pinfo->nextPageInfo;

			}
		else{
			pinfo=pinfo->nextPageInfo;		
		}
	}

	//if(i == q->totalNumOfFrames)
//	{
	//   pinfo = q->head;
	//  while(pinfo!= NULL)
	//  {
	//	  temp = pinfo;
		  //free(temp);
	//	  pinfo = pinfo->nextPageInfo;
	//  }
	
	  // return RC_OK;
  //  }
	//free(q);
	//free(fh);
    //	free(bm->mgmtData);
	//free(pinfo);
	//free(temp);
	closePageFile(fh);
	
	return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
	int i;
	pageInfo *temp1;
	temp1 = q->head;
	for(i=0; i< q->totalNumOfFrames; i++)
	{
		if((temp1->isDirty==1) && (temp1->fixCount==0))
		{
			writeBlock(temp1->pageNum,fh,temp1->bufferData);
			writeIO++;
			temp1->isDirty=0;
			//printf("\n----------Inside forceflush--------------");

		}

		temp1=temp1->nextPageInfo;
	}
	return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	pageInfo *pinfo;
	pinfo=searchQueue(page);
	if(pinfo==NULL)
		return RC_READ_NON_EXISTING_PAGE;		
	else
		pinfo->fixCount=pinfo->fixCount-1;
	return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
	pageInfo *pinfo;
	pinfo=searchQueue(page);
	int flag;

	if(pinfo==NULL)
		return 1;          //give error code
	if((flag=writeBlock(pinfo->pageNum,fh,pinfo->bufferData))==0)
		writeIO++;
	else
		return RC_WRITE_FAILED;

	return RC_OK;
}



pageInfo *searchPage(BM_BufferPool *const bm, BM_PageHandle *const page){
	pageInfo *temp;
	temp = q->head;
	int i;
	for(i=0; i < q->totalNumOfFrames;i++){
		if(temp->pageNum==page->pageNum)
			return temp;
		temp=temp->nextPageInfo;
	}
	return NULL;
}



RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	pageInfo *temp;
	temp = searchPage(bm, page);
	if(temp==NULL)
	return RC_READ_NON_EXISTING_PAGE;
	temp->isDirty=1;
	return RC_OK;
}




//------------Statistics Functions-----------------
PageNumber *getFrameContents (BM_BufferPool *const bm){
	PageNumber (*pages)[bm->numPages];
	pages=calloc(bm->numPages,sizeof(PageNumber));
	pageInfo *temp;
	temp = (*q).head;
int i;
	for(i=0; i<=q->filledframes;i++){
		(*pages)[i] = temp->pageNum;
		temp=temp->nextPageInfo;
}
	return *pages;

}

bool *getDirtyFlags (BM_BufferPool *const bm){
	bool (*isDirty)[bm->numPages];
	isDirty=calloc(bm->numPages,sizeof(PageNumber));
	pageInfo *temp;
	temp = (*q).head;
int i;
	for(i=0; i<=q->filledframes;i++){
		if(temp->isDirty==1){
			(*isDirty)[i]=TRUE;
		}
		else{
			(*isDirty)[i]=TRUE;
		}
		temp=temp->nextPageInfo;
	}
	return *isDirty;

	/*for(int i=0; i<=bm->numPages;i++){
		if(bufferInfo[i]->isDirty==1){
			(*isDirty)[i]= TRUE;
		}
		else{
			(*isDirty)[i]= FALSE;

	}
	return *isDirty;*/
}

int *getFixCounts (BM_BufferPool *const bm){
	int (*fixCounts)[bm->numPages];
	fixCounts=calloc(bm->numPages,sizeof(PageNumber));
	pageInfo *temp;
	temp = (*q).head;
int i;
	for( i=0; i<=q->filledframes;i++){
		(*fixCounts)[i] = temp->fixCount;
		temp=temp->nextPageInfo;
	}
	return *fixCounts;
}

int getNumReadIO (BM_BufferPool *const bm){
	return readIO;
}

int getNumWriteIO (BM_BufferPool *const bm){
	return writeIO;
}


RC pinPageFIFO(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
		int pageFound = 0,numPages;
		numPages = bm->numPages;
			pageInfo *list=NULL,*temp=NULL,*temp1=NULL;
			list = q->head;
			for (int i = 0; i < numPages; i++) {
				if (pageFound == 0) {
					if (list->pageNum == pageNum) {
						pageFound = 1;
						break;
					}
					else
					list = list->nextPageInfo;
				}
			}

			if (pageFound == 1 )
			{
				list->fixCount++;
				page->data = list->bufferData;
				return RC_OK;
			}
			
				temp = q->head;
				int returncode = -1;
				
					//printf("\n\n q->filledframes %d\n",q->filledframes);
								//printf("\n\n q->totalNumOfFrames %d\n",q->totalNumOfFrames);
									

			while (q->filledframes < q->totalNumOfFrames)
			{
				if(temp->pageNum == -1)
				{
			temp->fixCount = 1;
			temp->isDirty = 0;
			temp->pageNum = pageNum;
			page->pageNum= pageNum;
			temp->bufferData = bm->mgmtData;
			q->filledframes = q->filledframes + 1 ;
			
			readBlock(temp->pageNum,fh,temp->bufferData);
			//printf("\n\n temp->bufferData=%s hello \n",temp->bufferData);
			page->data = temp->bufferData;
			readIO++;
			returncode = 0;
			break;
 				}
			else
				temp = temp->nextPageInfo;
			}
			
			if(returncode == 0)
			return RC_OK;
		
		
			pageInfo *addnode = (pageInfo *) malloc (sizeof(pageInfo));
			addnode->fixCount = 1;
			addnode->isDirty = 0;
			addnode->pageNum = pageNum;
			addnode->bufferData = NULL;
			addnode->nextPageInfo = NULL;
			page->pageNum= pageNum;
			addnode->prevPageInfo = q->tail;
			temp = q->head;
			int i;
			for(i=0; i<numPages ;i++)
			{
				if((temp->fixCount)== 0)
					break;
	        else
				temp = temp->nextPageInfo;
			}

			if(i==numPages)
			{
				return RC_NO_FREE_BUFFER_ERROR;
			}

			temp1=temp;
				if(temp == q->head)
				{

					q->head = q->head->nextPageInfo;
					q->head->prevPageInfo = NULL;

				}
				else if(temp == q->tail)
				{
					q->tail = temp->prevPageInfo;
					addnode->prevPageInfo=q->tail;
				}
				else{
					temp->prevPageInfo->nextPageInfo = temp->nextPageInfo;
					temp->nextPageInfo->prevPageInfo=temp->prevPageInfo;
					}

				if(temp1->isDirty == 1)
				{
				 writeBlock(temp1->pageNum,fh,temp1->bufferData);
				 writeIO++;
				}
			addnode->bufferData = temp1->bufferData;
			addnode->frameNum = temp1->frameNum;
			
			readBlock(pageNum,fh,addnode->bufferData);
			page->data = addnode->bufferData;
			readIO++;
			
			q->tail->nextPageInfo = addnode;
			q->tail=addnode;
           return RC_OK;

}