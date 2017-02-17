#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"

FILE *file;

void initStorageManager(void) {
	printf("\n <---------------Initializing Storage Manager----------------->\n ");
	printf("BY\n");
	printf("Akash Gupta (A20378676) \n");
	printf("Tanmay Pradhan (A20376280) \n");
    	printf("Snehita kolapkar(A20389074) \n");

}

//Creating Page file
RC createPageFile(char *fileName) {
	RC returncode;
	file = fopen(fileName, "w+"); //Opening file having name as filename in write mode
	char *memoryBlock = malloc(PAGE_SIZE * sizeof(char)); //Memory Block initialized with size PAGE_SIZE

	if (file == 0)
		returncode = RC_FILE_NOT_FOUND;

	else {
		memset(memoryBlock, '\0', PAGE_SIZE); //Using memset function setting the allocated memory block by \0 if the file exists
		fwrite(memoryBlock, sizeof(char), PAGE_SIZE, file);	//Writing the allocated memory block in the file
		free(memoryBlock);		//Freeing the memoryBlock after writing
		fclose(file);			//Close file after creating it is done
		returncode = RC_OK;
	}

	return returncode;
}

//Opening Page file
RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
	file = fopen(fileName, "r+");		//Opening the file filename in read mode

	if (file == 0)
		return RC_FILE_NOT_FOUND;

	else {
		fseek(file, 0, SEEK_END);//fseek will point the file pointer to the last location of the file
		int endByte = ftell(file); 		//ftell returns the last byte of file
		int totalLength = endByte + 1;
		int totalNumPages = totalLength / PAGE_SIZE; //Total number of pages in the file

		(*fHandle).fileName = fileName; //Initializing the file attributes like fileName, total Number of Pages and current Page Position
		(*fHandle).totalNumPages = totalNumPages;
		(*fHandle).curPagePos = 0;
		rewind(file); //rewind sets the file pointer back to the start of the file
		return RC_OK;
	}
}

//Closing Page file
RC closePageFile(SM_FileHandle *fHandle) {
	RC isFileClosed;
	isFileClosed = fclose(file);//fclose closes the file successfully and returns 0
	if (isFileClosed == 0)
		return RC_OK;
	else
		return RC_FILE_NOT_FOUND;
}

//Destroying Page file
RC destroyPageFile(char *fileName) {
	if (remove(fileName) != 0)//remove will delete the file fileName and return 0 if successful
		return RC_FILE_NOT_FOUND;
	else
		return RC_OK;
}

//Reading pageNum^th block from the file
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC returncode;
	RC read_size;

	if ((*fHandle).totalNumPages < pageNum)	//If the total no. of pages are more than pageNum throw the error
		returncode = RC_READ_NON_EXISTING_PAGE;

	else {
		fseek(file, pageNum * PAGE_SIZE, SEEK_SET);
		read_size = fread(memPage, sizeof(char), PAGE_SIZE, file);
		if (read_size < PAGE_SIZE || read_size > PAGE_SIZE) {//If the block returned by fread() is not within the Page size limit, throw an error
			returncode = RC_READ_NON_EXISTING_PAGE;
		}
		(*fHandle).curPagePos = pageNum;//Update current page position to pageNum value
		returncode = RC_OK;
	}

	return returncode;
}

//Get current block position
int getBlockPos(SM_FileHandle *fHandle) {
	RC block_pos;
	block_pos = ((*fHandle).curPagePos);	//get current page position
	return block_pos;
}

//Reading first block from the file
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC returncode;
	if (fHandle == NULL)
		returncode = RC_FILE_NOT_FOUND;

	else {
		if ((*fHandle).totalNumPages <= 0)
			returncode = RC_READ_NON_EXISTING_PAGE;
		else {
			RC first_block;
			fseek(file, 0, SEEK_SET);
			first_block = fread(memPage, sizeof(char), PAGE_SIZE, file); //fread returns the first block of size PAGE_SIZE
			(*fHandle).curPagePos = 0; //First page of the file has index 0

			if (first_block < 0 || first_block > PAGE_SIZE) //If the read block is not within pagesize limits, throw an error
				returncode = RC_READ_NON_EXISTING_PAGE;

			returncode = RC_OK;
		}
	}
	return returncode;
}

//Reading previous block in file
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC returncode;

	if (fHandle == NULL)
		returncode = RC_FILE_NOT_FOUND;

	else {

		RC previous_block;
		RC prev_blockread;

		previous_block = (*fHandle).curPagePos - 1; //storing previous block index i.e 1 less than current page position

		if (previous_block < 0)
			returncode = RC_READ_NON_EXISTING_PAGE;

		else {
			fseek(file, (previous_block * PAGE_SIZE), SEEK_SET); //fseek will point the file pointer to the start of the previous block
			prev_blockread = fread(memPage, sizeof(char), PAGE_SIZE, file); //fread returns the read block
			(*fHandle).curPagePos = (*fHandle).curPagePos - 1; //Update the current page position reducing it by 1

			if (prev_blockread < 0 || prev_blockread > PAGE_SIZE)
				returncode = RC_READ_NON_EXISTING_PAGE;

			returncode = RC_OK;
		}
	}
	return returncode;
}

//Reads the current block which the file pointer points to
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC returncode;

	if (fHandle == NULL)	//If the fhandle is null, throw an error
		returncode = RC_FILE_HANDLE_NOT_INIT;

	else {
		RC current_block;
		RC curr_blockread;

		current_block = getBlockPos(fHandle); //get current block position to read
		fseek(file, (current_block * PAGE_SIZE), SEEK_SET);
		curr_blockread = fread(memPage, sizeof(char), PAGE_SIZE, file); //fread returns the current block to be read
		(*fHandle).curPagePos = current_block;

		if (curr_blockread < 0 || curr_blockread > PAGE_SIZE)
			returncode = RC_READ_NON_EXISTING_PAGE;

		returncode = RC_OK;
	}
	return returncode;
}

//Reading last block of the file
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC returncode;

	if (fHandle == NULL)
		returncode = RC_FILE_HANDLE_NOT_INIT;

	else {
		RC next_block;
		RC next_blockread;

		next_block = (*fHandle).curPagePos + 1; //get next block position
		if (next_block < (*fHandle).totalNumPages) {

			fseek(file, (next_block * PAGE_SIZE), SEEK_SET); //fseek points the file pointer to the next block
			next_blockread = fread(memPage, sizeof(char), PAGE_SIZE, file);

			(*fHandle).curPagePos = next_block; //Update current page position to next block index

			if (next_blockread < 0 || next_blockread > PAGE_SIZE)
				returncode = RC_READ_NON_EXISTING_PAGE;

			returncode = RC_OK;
		} else
			return RC_READ_NON_EXISTING_PAGE;

	}
	return returncode;
}

//Reading the last block of the file
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC returncode;

	if (fHandle == NULL)
		returncode = RC_FILE_NOT_FOUND;

	else {
		RC find_last_block;
		RC read_last_block;

		find_last_block = (*fHandle).totalNumPages - 1; //Last block has index 1 less than total no. of pages

		fseek(file, (find_last_block * PAGE_SIZE), SEEK_SET);
		read_last_block = fread(memPage, sizeof(char), PAGE_SIZE, file);

		(*fHandle).curPagePos = find_last_block; //Update current page position to the last block index

		if (read_last_block < 0 || read_last_block > PAGE_SIZE)
			returncode = RC_READ_NON_EXISTING_PAGE;

		returncode = RC_OK;
	}
	return returncode;
}

//Writing block at page pageNum
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC returncode;
	if (pageNum < 0 || pageNum > (*fHandle).totalNumPages) //If the pageNum is greater than totalNumber of pages, or less than zero, throw an error
		returncode = RC_WRITE_FAILED;

	else

	if (fHandle == NULL)
		returncode = RC_FILE_HANDLE_NOT_INIT;

	else {
		if (file != NULL) { //If file is not found, throw an error
			if (fseek(file, (PAGE_SIZE * pageNum), SEEK_SET) == 0) {
				fwrite(memPage, sizeof(char), PAGE_SIZE, file);

				(*fHandle).curPagePos = pageNum; //Update current page position to the pageNum i.e. index of the block written

				fseek(file, 0, SEEK_END);
				(*fHandle).totalNumPages = ftell(file) / PAGE_SIZE; //update the value of totalNumPages which increases by 1

				returncode = RC_OK;
			} else {
				returncode = RC_WRITE_FAILED;
			}
		} else {
			returncode = RC_FILE_NOT_FOUND;
		}
	}
	return returncode;

}

//Writing current block in the file
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC cur_position;
	cur_position = getBlockPos(fHandle); //get current block position

	RC write_curblock;
	write_curblock = writeBlock(cur_position, fHandle, memPage); //call the writeBlock function to write block at current position

	if (write_curblock == RC_OK)
		return RC_OK;

	else
		return RC_WRITE_FAILED;

}

//Append empty block to the file
RC appendEmptyBlock(SM_FileHandle *fHandle) {

	int returncode;
	if (file != NULL) {
		RC size = 0;

		char *newemptyblock;
		newemptyblock = (char *) calloc(PAGE_SIZE, sizeof(char)); //Create and initialize empty block

		fseek(file, 0, SEEK_END);
		size = fwrite(newemptyblock, 1, PAGE_SIZE, file);

		if (size == PAGE_SIZE) {
			(*fHandle).totalNumPages = ftell(file) / PAGE_SIZE; //update total no. of pages i.e. increase by 1
			(*fHandle).curPagePos = (*fHandle).totalNumPages - 1; //update current page position i.e. index is 1 less than totalNumPages
			returncode = RC_OK;
		}

		else {
			returncode = RC_WRITE_FAILED;
		}

		free(newemptyblock);

	} else {
		returncode = RC_FILE_NOT_FOUND;
	}
	return returncode;
}

//Ensure capacity of the file is numberOfPages
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {

	int pageNum = (*fHandle).totalNumPages;
	int i;

	if (numberOfPages > pageNum) { //If the new capacity is greater than current capacity
		int add_page = numberOfPages - pageNum;
		for (i = 0; i < add_page; i++) //Add add_page no. of pages by calling the appendEmptyBlock function
			appendEmptyBlock(fHandle);

		return RC_OK;
	}

	else
		return RC_WRITE_FAILED;
}