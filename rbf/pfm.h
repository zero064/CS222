#ifndef _pfm_h_
#define _pfm_h_

#include <string>
#include <climits>

typedef int RC;
typedef char byte;
typedef unsigned PageNum;

// Directory Page 
struct Directory{
    unsigned pagenum;
    short int freespace;
};
// Directory Page Descriptor
struct DirectoryDesc{
    unsigned nextDir;
    short int size;
};


#define PAGE_SIZE 4096
#define SUCCESS 0
#define FAILURE -1

#ifdef NDEBUG
#define debug_print(fmt, ...) fprintf(stderr, fmt, __VA_ARGS__)
#else
#define debug_print(fmt, ...) do {} while (0)
#endif

using namespace std;

class FileHandle;


class PagedFileManager
{
public:
    static PagedFileManager* instance();                     // Access to the _pf_manager instance

    RC createFile    (const string &fileName);                         // Create a new file
    RC destroyFile   (const string &fileName);                         // Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                         // Close a file

protected:
    PagedFileManager();                                   // Constructor
    ~PagedFileManager();                                  // Destructor

private:
    static PagedFileManager *_pf_manager;

};


class FileHandle
{
public:
    // variables to keep counter for each operation
	unsigned readPageCounter;
	unsigned writePageCounter;
	unsigned appendPageCounter;
	
    FileHandle();                                                    // Default constructor
    ~FileHandle();                                                   // Destructor

    // init filePointer
    RC initFilePointer(const string &fileName);
    RC closeFilePointer();

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // put the current counter values into variables

private:
    FILE *filePointer;  // a file pointer to write Pages into disk
    unsigned numberOfPages;
}; 

#endif
