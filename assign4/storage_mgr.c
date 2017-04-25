#include "storage_mgr.h"
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include "test_helper.h"

//Global Data Structures [START]
typedef struct _metaDataList // data structure to metaData as a Linked List of Maps.
{
		char key[50];
		char value[50];
		struct _metaDataList *nextMetaDataNode;
} metaDataList;

metaDataList *firstNode = NULL;
metaDataList *currentNode = NULL;
metaDataList *previousNode = NULL;

int whoIsCallingCreate = 1; // 1--> test case calls, else -->any other function
//Global Data Structures [END]

void initStorageManager()
{
	printf("\n <---------------Initializing Storage Manager----------------->\n ");
	printf("BY\n");
	printf("Akash Gupta (A20378676) \n");
	printf("Tanmay Pradhan (A20376280) \n");
    printf("Snehita kolapkar(A20389074) \n");
}


void getMeNthMetaData(int n, char * string,char *nthKeyValuePair)
{
	char newString[PAGE_SIZE];
	memset(newString, '\0', PAGE_SIZE);
	newString[0] = ';';
	strcat(newString, string);

	//store position of ; in arrays, say delimiterPosition [START]
	char delimiterPosition[1000];
	int iLoop;
	int delPostion = 0;

	for (iLoop = 0; iLoop < strlen(newString); iLoop++)
	{
		if (newString[iLoop] == ';')
		{
			delimiterPosition[delPostion] = iLoop;
			delPostion++;
		}
	}
	//store position of ; in arrays, say delimiterPosition [END]

	int currentPos = 0;
	for (iLoop = delimiterPosition[n - 1] + 1;
			iLoop <= delimiterPosition[n] - 1; iLoop++)
	{
		nthKeyValuePair[currentPos] = newString[iLoop];
		currentPos++;
	}
	nthKeyValuePair[currentPos] = '\0';
}


metaDataList * constructMetaDataLinkedList(char *metaInformation,
		int noOfNodesToBeConstructed)
{
	int iLoop;
	char currentMetaKeyValue[100];

	char currentKey[50];
	memset(currentKey,'\0',50);

	char currentValue[50];
	memset(currentValue,'\0',50);

	for (iLoop = 1; iLoop <= noOfNodesToBeConstructed; iLoop++)
	{
		memset(currentMetaKeyValue,'\0',100);
		getMeNthMetaData(iLoop, metaInformation,currentMetaKeyValue);
		//Split key ,values, store it in 2 variables

		char colonFound = 'N';
		int keyCounter = 0;
		int ValueCounter = 0;
		int i;
		for (i = 0; i < strlen(currentMetaKeyValue); i++)
		{
			if (currentMetaKeyValue[i] == ':')
				colonFound = 'Y';

			if (colonFound == 'N')
				currentKey[keyCounter++] = currentMetaKeyValue[i];
			else if (currentMetaKeyValue[i] != ':')
				currentValue[ValueCounter++] = currentMetaKeyValue[i];
		}
		currentKey[keyCounter] = '\0';
		currentValue[ValueCounter] = '\0';

		//Ex PS:4096

		currentNode = (metaDataList *) malloc(sizeof(metaDataList));

		strcpy(currentNode->value,currentValue);
		strcpy(currentNode->key,currentKey);
		currentNode->nextMetaDataNode = NULL;

		if (iLoop == 1)
		{
			firstNode= currentNode;
			previousNode = NULL;
		}
		else
		{
			previousNode->nextMetaDataNode = currentNode;
		}
		previousNode = currentNode;
	}
	return firstNode;
}

RC createPageFile(char *filename)
{
	FILE *fp;
	fp = fopen(filename, "a+b"); // create and open the file for read/write

	if (whoIsCallingCreate == 1) // If Test case calls this function, reserve 3 blocks
	{
		if (fp != NULL)
		{
			char nullString2[PAGE_SIZE]; // 2nd metaBlock
			char nullString3[PAGE_SIZE]; // actual Data Block

			//to Store PageSize in string format.[start]
			char stringPageSize[5];
			sprintf(stringPageSize, "%d", PAGE_SIZE);

			char strMetaInfo[PAGE_SIZE * 2];
			strcpy(strMetaInfo, "PS:"); // PS == PageSize
			strcat(strMetaInfo, stringPageSize);
			strcat(strMetaInfo, ";");
			strcat(strMetaInfo, "NP:0;"); //NP == No of Pages
			//to Store PageSize in string format.[end]

			int i;
			for (i = strlen(strMetaInfo); i < (PAGE_SIZE * 2); i++)
				strMetaInfo[i] = '\0';
			memset(nullString2, '\0', PAGE_SIZE);
			memset(nullString3, '\0', PAGE_SIZE);

			fwrite(strMetaInfo, PAGE_SIZE, 1, fp);
			fwrite(nullString2, PAGE_SIZE, 1, fp);
			fwrite(nullString3, PAGE_SIZE, 1, fp);

			fclose(fp);
			return RC_OK;
		} else
		{
			return RC_FILE_NOT_FOUND;
		}
	}
	else
	{
		if (fp != NULL)
		{
			char nullString[PAGE_SIZE];

			memset(nullString, '\0', PAGE_SIZE);
			fwrite(nullString, PAGE_SIZE, 1, fp);

			fclose(fp);
			return RC_OK;
		} else
		{
			return RC_FILE_NOT_FOUND;
		}
	}

}

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
	struct stat statistics;
	FILE *fp;

	fp = fopen(fileName, "r");
	if (fp != NULL)
	{
		//Initialize our structure with required values.
		fHandle->fileName = fileName;
		fHandle->curPagePos = 0;
		stat(fileName, &statistics);
		fHandle->totalNumPages = (int) statistics.st_size / PAGE_SIZE;
		fHandle->totalNumPages -= 2; // 2 pages are reserved for metaInfo. Hence Subtracting.

		//Read MetaData Information and dump it into a Linked List [START]
		char metaDataInformationString[PAGE_SIZE * 2];
		fgets(metaDataInformationString, (PAGE_SIZE * 2), fp);
		//Read MetaData Information and dump it into a String [END]

		//Count the number of metaData Nodes to be constructed [START]
		int iLoop;
		int noOfNodes = 0;
		for (iLoop = 0; iLoop < strlen(metaDataInformationString); iLoop++)
			if (metaDataInformationString[iLoop] == ';')
				noOfNodes++;
		//Count the number of metaData Nodes to be constructed [END]

		fHandle->mgmtInfo = constructMetaDataLinkedList(
				metaDataInformationString, noOfNodes);
		//This fileHandle now has all the metaInfo needed.. Phew !

		//Read MetaData Information and dump it into a Linked List [END]
		fclose(fp);

		return RC_OK;
	}
	else
	{
		return RC_FILE_NOT_FOUND;
	}
}

void convertToString(int someNumber,char * reversedArray)
{
	char array[4];
	memset(array, '\0', 4);
	int i = 0;
	while (someNumber != 0)
	{
		array[i++] = (someNumber % 10) + '0';
		someNumber /= 10;
	}
	array[i] = '\0';

	int j=0;
	int x;
	for(x = strlen(array)-1;x>=0;x--)
	{
		reversedArray[j++] = array[x];
	}
	reversedArray[j]='\0';
}

RC writeMetaListOntoFile(SM_FileHandle *fHandle,char *dataToBeWritten)
{
	FILE *fp = fopen(fHandle->fileName,"r+b");

	if(fp != NULL)
	{
		fwrite(dataToBeWritten,1,PAGE_SIZE,fp);
		fclose(fp);
		return RC_OK;
	}
	else
	{
		return RC_WRITE_FAILED;
	}
}


void freeMemory()
{
	metaDataList *previousNode;
	metaDataList *current  = firstNode;
	previousNode = firstNode;
	while(current != NULL)
	{
		current = current->nextMetaDataNode;
		if(previousNode!=NULL)
			free(previousNode);
		previousNode = current;
	}
	previousNode = NULL;
	firstNode = NULL;
}

RC closePageFile(SM_FileHandle *fHandle)
{
	if (fHandle != NULL)
	{
		metaDataList *temp = firstNode;
		char string[4];
		memset(string,'\0',4);
		while (1 == 1)
		{
			if(temp != NULL)
			{
				if(temp->key != NULL)
				{
					if (strcmp(temp->key, "NP") == 0)
					{
						convertToString(fHandle->totalNumPages,string);
						strcpy(temp->value,string);
						break;
					}
				}
				temp = temp->nextMetaDataNode;
			}
			else
				break;
		}
		temp = firstNode;

		char metaData[2 * PAGE_SIZE];
		memset(metaData, '\0', 2 * PAGE_SIZE);
		int i = 0;
		while (temp != NULL)
		{
			int keyCounter = 0;
			int valueCounter = 0;
			while (temp->key[keyCounter] != '\0')
				metaData[i++] = temp->key[keyCounter++];
			metaData[i++] = ':';
			while (temp->value[valueCounter] != '\0')
				metaData[i++] = temp->value[valueCounter++];
			metaData[i++] = ';';
			temp = temp->nextMetaDataNode;
		}
		writeMetaListOntoFile(fHandle,metaData);
		fHandle->curPagePos = 0;
		fHandle->fileName = NULL;
		fHandle->mgmtInfo = NULL;
		fHandle->totalNumPages = 0;
		fHandle = NULL;
		freeMemory();
		return RC_OK;
	}
	else
	{
		return RC_FILE_HANDLE_NOT_INIT;
	}
}

RC destroyPageFile(char *fileName)
{
	if (remove(fileName) == 0)
		return RC_OK;
	else
		return RC_FILE_NOT_FOUND;
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	if (pageNum < 0 || pageNum > fHandle->totalNumPages)//
		return RC_WRITE_FAILED;
	else
	{
		int startPosition = (pageNum * PAGE_SIZE) + (2 * PAGE_SIZE);

		FILE *fp = fopen(fHandle->fileName, "r+b");
		if (fp != NULL)
		{
			if (fseek(fp, startPosition, SEEK_SET) == 0)
			{
				fwrite(memPage, 1, PAGE_SIZE, fp);
				if (pageNum > fHandle->curPagePos)
					fHandle->totalNumPages++;
				fHandle->curPagePos = pageNum;
				fclose(fp);
				return RC_OK;
			}
			else
			{
				return RC_WRITE_FAILED;
			}
		} else
		{
			return RC_FILE_HANDLE_NOT_INIT;
		}
	}
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (writeBlock(fHandle->curPagePos, fHandle, memPage) == RC_OK)
		return RC_OK;
	else
		return RC_WRITE_FAILED;
}

RC appendEmptyBlock(SM_FileHandle *fHandle)
{
	whoIsCallingCreate = 2;
	if (createPageFile(fHandle->fileName) == RC_OK)
	{
		//Changing the value of totalNumPages and curPagePos as we are adding new blocks
		fHandle->totalNumPages++;
		fHandle->curPagePos = fHandle->totalNumPages - 1;
		whoIsCallingCreate = 1;
		return RC_OK;
	}
	else
	{
		whoIsCallingCreate = 1;
		return RC_WRITE_FAILED;
	}
}


RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
	int extraPagesToBeAdded = numberOfPages - (fHandle->totalNumPages);
	int iLoop;
	if (extraPagesToBeAdded > 0)
	{
		for (iLoop = 0; iLoop < extraPagesToBeAdded; iLoop++)
		{
			whoIsCallingCreate = 3;
			createPageFile(fHandle->fileName);
			//Changing the value of totalNumPages and curPagePos as we are adding new blocks
			whoIsCallingCreate = 1;
			fHandle->totalNumPages++;
			fHandle->curPagePos = fHandle->totalNumPages - 1;
		}
		return RC_OK;
	} else
		return RC_READ_NON_EXISTING_PAGE; // this is the closest macro i could fit in here..
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//if pageNum is n, read n+2th page.
	if (fHandle->totalNumPages < pageNum)
	{
		return RC_READ_NON_EXISTING_PAGE;
	}
	else
	{
		FILE *fp;
		fp = fopen(fHandle->fileName, "r");
		
		if (fp != NULL)
		{
			if (fseek(fp, ((pageNum * PAGE_SIZE) + 2 * PAGE_SIZE), SEEK_SET)== 0)
			{
				fread(memPage, PAGE_SIZE, 1, fp);
				fHandle->curPagePos = pageNum;
				fclose(fp);
				return RC_OK;
			}
			else
			{
				return RC_READ_NON_EXISTING_PAGE;
			}
		} else
		{
			return RC_FILE_NOT_FOUND;
		}
	}
}

int getBlockPos(SM_FileHandle *fHandle)
{
	return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(0, fHandle, memPage) == RC_OK)
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}


RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(getBlockPos(fHandle) - 1, fHandle, memPage) == RC_OK)
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}


RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(getBlockPos(fHandle), fHandle, memPage) == RC_OK)
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(getBlockPos(fHandle) + 1, fHandle, memPage) == RC_OK)
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}


RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(fHandle->totalNumPages - 1, fHandle, memPage) == RC_OK)
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}