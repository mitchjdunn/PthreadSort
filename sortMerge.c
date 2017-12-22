#include <stdio.h>
#include <sys/sysinfo.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
//this is a structure to hold the lines in separate arrays
//key is the 8 bit key
//value is the 54 bit message
struct data
{
	char key[9];
	char value[55];
};
//this is a structure to pass information on to the threads.
//it consists of the starting index of the array, the number of thread left to create, and the size
//of the part of array they are supposed to work on.
struct threadInfo
{
	int init;
	int threads;
	int size;
};
struct data lines[512000];//raw-> sorted array. 512000 entries long to support the large file.
char sortedArray[512000][65];//the fully sorted array that is used to merge into.

//this method is used for qsort to compare the key for each line.
int comparator(const void * a, const void * b){
	char *aKey = ((struct data * )a)->key;
	char *bKey = ((struct data * )b)->key;
	for(int i = 0; i < 8; i++){
		if(aKey[i] < bKey[i]){
			return -1;
		}else if(aKey[i] > bKey[i]){
			return 1;
		}
	}
	return 0;
}

//the same as comparator but is callable within the program.
int order(struct data a, struct data b){
	for(int i = 0; i < 8; i++){
		if(a.key[i] < b.key[i]){
			return -1;
		}else if(a.key[i] > b.key[i]){
			return 1;
		}
	}
	return 0;
}
//uses qsort to sort part of the array
void sortArray(int startIndex, int size){
	int endIndex = startIndex + size;
	//printf("sortArray %d %d\n", startIndex, endIndex);
	qsort((void*)lines + (startIndex * sizeof(lines[0])), endIndex - startIndex, sizeof(lines[1]), comparator);
}
//uses qsort to sort part of the array, callable by thread.
void *sort(const void * info){
	int startIndex = ((struct threadInfo * )info)->init;
	int endIndex = startIndex + ((struct threadInfo * )info)->size;
	//printf("sortArray %d %d\n", startIndex, endIndex);
	qsort((void*)lines + (startIndex * sizeof(lines[0])), endIndex - startIndex, sizeof(lines[1]), comparator);
}
//creates threads to sort and merge parts of the array.
void *createThreads(const void * info){
	struct threadInfo newInfo;
	int init = ((struct threadInfo * )info)->init;
	int threads = ((struct threadInfo * )info)->threads;
	int size = ((struct threadInfo * )info)->size;
	newInfo.init = init;
	newInfo.size = size / 2;
	newInfo.threads = threads / 2;
	//printf("createThreads %d %d %d\n", threads, init, size);
	pthread_t id;
	if(threads == 2){
		pthread_create(&id, NULL, sort, &newInfo);
		sortArray(init + size/2, size / 2);
	}else{
		struct threadInfo otherInfo;
		otherInfo.init = init + newInfo.size;
		otherInfo.threads = newInfo.threads;
		otherInfo.size = size / 2;
		pthread_create(&id, NULL, createThreads, &newInfo);
		createThreads(&otherInfo);
	}
		pthread_join(id, NULL);
		merge(init, init + newInfo.size, init + newInfo.size, init + newInfo.size + size/2);
}
//puts everything together and records the time it takes to run.
int main(int argc, char *argv[])
{
	clock_t begin = clock();
	clock_t end;
	double time;
	int nCores = get_nprocs();
	char *fileName;
	int nThreads,lineCount;
	printf("cores: %d\n", nCores);
	struct threadInfo info;
	if(argc == 3){
		nThreads = atoi(argv[1]);
		fileName = argv[2];
		printf("number of threads %d\n", nThreads);
		printf("file to sort %s\n", fileName);
	}else{
		printf("USAGE: %s <number of threads> <//path//to//file>", argv[0]);
		//specifically ask user for input
		return 1;
	}
	if(nCores <= 2 && nThreads > 8){
		printf("number of threads has to be less than or equal to 8 for %d cores\n", nCores);
		return 1;
	}
	if(!powerOfTwo(nThreads)){
			printf("number of threads has to be a power of two\n");
			return 1;
		
	}
	printf("%s\n", fileName);
	lineCount = readFileToArray(fileName);
	info.init = 0;
	info.threads = nThreads;
	info.size = lineCount;
	pthread_t id;
	if(pthread_create(&id, NULL, createThreads, &info)){
	 	printf("Error creating thread\n");
		return 1;
	}
	if(pthread_join(id, NULL)){
	 	printf("Error joining threads \n");
	 	return 1;
	}
	end = clock();
	time = (double)(end - begin) / CLOCKS_PER_SEC;
	printSortedArray(lineCount);
	printf("Time spent: %f\n", time);
	
	return 0;
}

//returns 1 if the argument is a power of 2
int powerOfTwo(int num){
	if (num == 0)
		return 0;
	while(num != 1){
		if(num%2 != 0)
			return 0;
		num = num/2;
	}
	return 1;
}
//prints sortedArray
void printSortedArray(int lineCount){
	for(int i = 0; i < lineCount; i++){
		printf("%s\n", sortedArray[i]);
	}
}
//print the struct data array
void printArray(int lineCount){
	for(int i = 0; i < lineCount; i++)
	{
		printf("%d  %s %s\n", i, lines[i].key, lines[i].value);
	}	
}

//merges two adjacent parts of the array into sortedArray
void merge(int startIndex1, int endIndex1, int startIndex2, int  endIndex2){
	//printf("merge %d %d %d %d\n", startIndex1, endIndex1, startIndex2, endIndex2);
	int index = startIndex1;
	int start = startIndex1;
	while(startIndex1 < endIndex1 && startIndex2 < endIndex2){
		if(order(lines[startIndex1],lines[startIndex2]) == 1){
			memcpy(sortedArray[index], lines[startIndex2].key, 8);
			memcpy(sortedArray[index] + 8, lines[startIndex2].value, 55);
			index++;
			startIndex2++;
		}else{
			memcpy(sortedArray[index], lines[startIndex1].key, 8);
			memcpy(sortedArray[index] + 8, lines[startIndex1].value, 55);
			index++;
			startIndex1++;
		}
	}
	if(startIndex1 == endIndex1){
		while(startIndex2 < endIndex2){
			memcpy(sortedArray[index], lines[startIndex2].key, 8);
			memcpy(sortedArray[index] + 8, lines[startIndex2].value, 55);
			index++;
			startIndex2++;
		}
	}else{
		while(startIndex1<endIndex1){
			memcpy(sortedArray[index], lines[startIndex1].key, 8);
			memcpy(sortedArray[index] + 8, lines[startIndex1].value, 55);
			index++;
			startIndex1++;
		}
	}
	//this is why it is slow -> best guess.
	copyMergedToUnsorted(startIndex1, endIndex2);
}

//Used when merging more than 2 sections.  fills the original array with the already
//merged array
void copyMergedToUnsorted(int startIndex, int endIndex){
	for(startIndex; startIndex < endIndex; startIndex++){
		memcpy(lines[startIndex].key, sortedArray[startIndex], 8);
		memcpy(lines[startIndex].value, sortedArray[startIndex] + 8, 55);
	}
}

//Stores file into an array of type struct data
int readFileToArray(char *fileName)
{
	char buff[65];

	FILE *fp;
	fp = fopen(fileName, "r");


	int lineCount = 0;
	while(fgets(buff, 65, fp) != NULL)
	{
		memcpy(lines[lineCount].key, buff, 8);
		memcpy(lines[lineCount].value, buff + 8, 54);
		lines[lineCount].key[8] = '\0';
		lines[lineCount].value[54] = '\0';
		lineCount++;
	}
	fclose(fp);
	return lineCount;
}
