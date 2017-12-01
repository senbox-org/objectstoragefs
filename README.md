# objectstoragefs

Provides additional Java NIO [FileSystem](https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileSystem.html)
implementations for [Object Storage](https://en.wikipedia.org/wiki/Object_storage) based file systems.

## Usage

Create file system for an AWS S3 bucket:

    FileSystem fs = FileSystems.newFileSystem("s3:http://sentinel-s2-l1c.s3.amazonaws.com", new HashMap());
    
Get path to a file in the file system:

    Path filePath = fs.getPath("tiles/1/C/CV/2015/12/21/0/B12.jp2");

Read entire contents:

    byte[] data = Files.readAllBytes(filePath);

Read byte ranges:

    try (SeekableByteChannel channel = Files.newByteChannel(filePath)) {
        ...
    }
    
List directories:

    Stream<Path> entries = Files.list(fs.getPath("tiles/1/C/CV/2015/12/21/0/"));


## Status

Currently supported Object Storage systems:

* [Amazon S3](https://aws.amazon.com/s3/?nc1=h_ls)

This project's code is still experimental and the `FileSystem` implementations are incomplete.
The following limitations apply:

* File systems have no authentication support yet, therefore only public buckets can be accessed.
* File systems are currently read-only, so the following methods are not implemented and will throw `UnsupportedOperationException`:
  * `ObjectStorageFileSystemProvider.createDirectory()`  
  * `ObjectStorageFileSystemProvider.delete()`  
  * `ObjectStorageFileSystemProvider.copy()`  
  * `ObjectStorageFileSystemProvider.move()`  
  * `ObjectStorageFileSystemProvider.setAttribute()`  
* Other missing method implementations that will throw `UnsupportedOperationException`:
  * `ObjectStorageFileSystemProvider.getFileStore()`  
  * `ObjectStorageFileSystemProvider.getFileAttributeView()`  
  * `ObjectStorageFileSystemProvider.checkAccess()`  
  * `ObjectStorageFileSystemProvider.readAttributes(): Map`  
  * `ObjectStorageFileSystem.getFileStores()`  
  * `ObjectStorageFileSystem.getPathMatcher()`  
  * `ObjectStorageFileSystem.getUserPrincipalLookupService()`  
  * `ObjectStorageFileSystem.supportedFileAttributeViews()`  
  * `ObjectStorageFileSystem.newWatchService()`  


## Roadmap

Add support for (in this order):

1. Authentication
2. File attribute views
3. Watch service
4. Path matching with globs and filters
5. Writing to files

