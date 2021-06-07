import java.io.*;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.util.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

//下载文件
public class Main {
    //    private final static String bucketName = "zhangziyi";
//    private final static String filePath1   =
//            "C:\\Users\\86733\\Desktop\\index.html";
    private final static String accessKey = "B67DDB9A1DCDE1F208B4";
    private final static String secretKey =
            "WzZGQ0MwOUEwNTlFNjI2RjgwMTkzQUZERkIwRDgy";
    private final static String serviceEndpoint =
            "http://scut.depts.bingosoft.net:29997";
    private final static long partSize = 5 << 20;
    private final static String signingRegion = "";
    private final static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {

        final BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        final ClientConfiguration ccfg = new ClientConfiguration().
                withUseExpectContinue(true);

        final EndpointConfiguration endpoint = new EndpointConfiguration(serviceEndpoint, signingRegion);

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withClientConfiguration(ccfg)
                .withEndpointConfiguration(endpoint)
                .withPathStyleAccessEnabled(true)
                .build();

        //  控制台获取要同步的本地目录和s3上的桶
        System.out.print("请输入要同步的本地目录：");
//        String synLocalPath = scanner.nextLine();
        String synLocalPath = "G:/Bingo Big Data Development/synchronization directory/";
        System.out.println("\n同步的本地目录：" + synLocalPath);

        System.out.print("请输入要同步的s3存储桶：");
//        String bucketName = scanner.nextLine();
        String bucketName = "luna";
        System.out.println("\n同步的s3存储桶：" + bucketName);

        //获取s3中的对象，路径从根目录开始
        List<String> objectPathList = new ArrayList<String>();
        ObjectListing objects = s3.listObjects(bucketName);
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                System.out.println("Object: " + objectSummary.getKey());
                objectPathList.add(objectSummary.getKey());
            }
            objects = s3.listNextBatchOfObjects(objects);
            System.out.println(objects.isTruncated());
        } while (objects.isTruncated());  //  这里判断是否是可裁剪的，也就是递归地遍历每个文件夹


        //  将s3的文件同步到本地
        S3ObjectInputStream s3is = null;
        FileOutputStream fos = null;
        try {
            System.out.println("请稍候，正在同步文件...");

            for (String objectPath : objectPathList) {
                //  首先，文件夹不存在的话应该被新建
                if (objectPath.contains("/")) {
                    System.out.println(objectPath);
                    String[] dirs = objectPath.split("/");
                    StringBuilder dir = new StringBuilder(synLocalPath);
                    for (int i = 0; i < dirs.length; ++i) {
                        if (!dirs[i].contains(".")) {
                            dir.append(dirs[i]).append("/");
                        }
                    }
                    File file = new File(dir.toString());
                    if (!file.exists()) {
                        file.mkdirs();
                    }
                }

                //  处理文件冲突
                // 冲突解决策略：
                // 1. 文件不存在的时候，直接下载；
                // 2. 存在同名文件时，让用户判断是保留本地文件，还是下载云端文件进行覆盖。
                //    --如果选择下载云端文件，那么将会覆盖本地文件；
                //    --如果选择保留本地文件，那么将会将本地文件上传到云端，覆盖云端文件。

                File file = new File(synLocalPath + "/" + objectPath);
                if (!file.exists()) {
                    //  然后开始下载文件夹中的对应文件
                    System.out.println("正在下载：" + objectPath);
                    S3Object o = s3.getObject(bucketName, objectPath);
                    s3is = o.getObjectContent();
                    if (o.getObjectMetadata().getContentLength() > partSize) {
                        download_blocked(s3, bucketName, file, synLocalPath);
                    } else {
                        fos = new FileOutputStream(new File(synLocalPath + "/" + objectPath));
                        byte[] read_buf = new byte[64 * 1024];
                        int read_len = 0;
                        while ((read_len = s3is.read(read_buf)) > 0) {
                            fos.write(read_buf, 0, read_len);
                        }
                        fos.close();
                        s3is.close();
                    }
                    System.out.println(objectPath + " 下载完成！");

                } else if (!file.isDirectory()) {
                    int choice = 0;
                    System.out.println("存在同名文件 “" + objectPath + "” ，请决定" +
                            "是保留本地文件还是下载云端文件进行覆盖。");
                    System.out.println("请注意，云端文件和本地文件将会进行同步。");
                    System.out.println("其他数字. 保留本地文件，覆盖云端文件；" +
                            "2. 下载云端文件，覆盖本地文件。");
                    System.out.print("请键入数字进行选择：");
                    choice = scanner.nextInt();
                    if (choice == 2) {
                        System.out.println("本地文件将被覆盖。");
                        System.out.println("正在下载：" + objectPath);
                        S3Object o = s3.getObject(bucketName, objectPath);
                        if (o.getObjectMetadata().getContentLength() > partSize) {
                            download_blocked(s3, bucketName, file, synLocalPath);
                        } else {
                            s3is = o.getObjectContent();
                            fos = new FileOutputStream(new File(synLocalPath + "/" + objectPath));
                            byte[] read_buf = new byte[64 * 1024];
                            int read_len = 0;
                            while ((read_len = s3is.read(read_buf)) > 0) {
                                fos.write(read_buf, 0, read_len);
                            }
                            fos.close();
                            s3is.close();
                        }
                        System.out.println("已完成本地文件 " + objectPath + " 的覆盖。");
                    } else {
                        s3.putObject(bucketName, objectPath, file);
                    }
                }
            }

            System.out.println("云端文件本地同步完成~");
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } finally {
            if (s3is != null) try {
                s3is.close();
            } catch (IOException e) {
            }
            if (fos != null) try {
                fos.close();
            } catch (IOException e) {
            }
        }


        //  接下来将实现本地文件的新建、删除、修改操作同步到云端
        System.out.println("接下来将对本地文件的增删改进行云端同步");
        int flag = 0;
        Map<String, Long> historyFileModifiedTimeList = new HashMap<>();
        List<String> nowFilePathList = null;
        List<String> objectNowList = null;
        while (true) {
            try {
                if (flag == 0) {
                    System.out.println("请注意，该功能为一有条件无限循环。");
                    flag = 1;
                }
                System.out.println("请选择功能：");
                System.out.println("其他数字. 本地文件云端同步；0. 退出(默认)。");
                int choice = 0;
                choice = scanner.nextInt();
                if (choice == 0) {
                    System.out.println("感谢您的使用！再见！");
                    break;
                } else {
                    try {
                        //  获取此时的本地同步目录
                        nowFilePathList = getFilePathFromDir(synLocalPath);
                        //  获取此时的云端目录
                        objectNowList = new ArrayList<String>();
                        ObjectListing objectsNow = s3.listObjects(bucketName);
                        do {
                            for (S3ObjectSummary objectSummary : objectsNow.getObjectSummaries()) {
//                                System.out.println("Object: " + objectSummary.getKey());
                                objectNowList.add(objectSummary.getKey());
                            }
                            objectsNow = s3.listNextBatchOfObjects(objects);
                        } while (objectsNow.isTruncated());
                        //  ↑这里判断是否是可裁剪的，也就是递归地遍历每个文件夹

                        //  这里预计是处理本地添加的文件
                        for (String localFile : nowFilePathList) {
                            if (!objectNowList.contains(localFile)) {
                                File file = new File(synLocalPath + localFile);
                                if (file.exists() && !file.isDirectory()) {
                                    //  遍历本地文件，对云端不存在的、本地新添加的文件进行上传
                                    //  对于已经存在的文件，可以实现直接覆盖
                                    System.out.println("将本地新添加的文件 " + localFile + " 进行上传");
                                    if (file.length() > partSize /** 4*/) {
//                                        System.out.println(localFile);
//                                        System.out.println(file.getCanonicalPath());
                                        System.out.println("文件 " + localFile +
                                                " 较大，将进行分块上传。");
                                        System.out.println("正在分块上传文件 " +
                                                localFile + " ，请稍后...");
                                        upload_blocked(s3, bucketName, localFile, file);
                                        System.out.println("文件 " + localFile + " 上传云端成功！");
                                    } else {
                                        System.out.println("正在上传文件 " + localFile + " ，请稍后...");
                                        s3.putObject(bucketName, localFile, file);
                                        System.out.println("文件 " + localFile + " 上传云端成功！");
                                    }
                                    System.out.println("文件上传" + localFile + "成功！");
                                    historyFileModifiedTimeList.put(file.getCanonicalPath(), file.lastModified());
                                }
                            }
                        }


                        //  检测本地文件是否有修改
                        List<File> nowFileList = getFilesFromDir(synLocalPath);
                        if (!historyFileModifiedTimeList.isEmpty()) {
                            System.out.println("开始检测本地文件的修改...");
                            for (File file : nowFileList) {
                                //  异常出现在下一行
//                                System.out.println(file);
//                                System.out.println(historyFileModifiedTimeList.
//                                        get(file.getCanonicalPath()));
                                if (!file.isDirectory() && (historyFileModifiedTimeList.
                                        get(file.getCanonicalPath()) != file.lastModified())) {
                                    String localPath = file.getCanonicalPath().
                                            replace("\\", "/").
                                            replace(synLocalPath, "");
                                    System.out.println("本地文件 " + localPath + " 已被" +
                                            "修改，进行云端同步...");
                                    if (file.length() > partSize /** 4*/) {
//                                        System.out.println(localFile);
//                                        System.out.println(file.getCanonicalPath());
                                        System.out.println("文件 " + localPath +
                                                " 较大，将进行分块上传。");
                                        System.out.println("正在分块上传文件 " +
                                                localPath + " ，请稍后...");
                                        upload_blocked(s3, bucketName, localPath, file);
                                        System.out.println("文件 " + localPath + " 上传云端成功！");
                                    } else {
                                        System.out.println("正在上传文件 " + localPath + " ，请稍后...");
                                        s3.putObject(bucketName, localPath, file);
                                        System.out.println("文件 " + localPath + " 上传云端成功！");
                                    }
                                    System.out.println("文件 " + localPath + " 云端同步完成！");
                                    break;
                                }
                            }
                            System.out.println("本地文件的修改检测完成！");
                        } else {
                            System.out.println("本地文件目录无历史信息！");
                        }
                        for (File file : nowFileList) {
                            //  针对先前已有的key，新value会覆盖之前的value
                            if (!file.isDirectory()) {
                                historyFileModifiedTimeList.put(file.getCanonicalPath(),
                                        file.lastModified());
                            }
                        }
                        //  nowFileList.clear();

                        for (File file : nowFileList) {

                        }

                        //  这里预计是处理本地删除的文件
                        //  获取此时的云端文件目录
                        do {
                            for (S3ObjectSummary objectSummary : objectsNow.getObjectSummaries()) {
//                                System.out.println("Object: " + objectSummary.getKey());
                                objectNowList.add(objectSummary.getKey());
                            }
                            objectsNow = s3.listNextBatchOfObjects(objects);
                        } while (objectsNow.isTruncated());
                        //  ↑这里判断是否是可裁剪的，也就是递归地遍历每个文件夹

                        nowFilePathList = getFilePathFromDir(synLocalPath);

                        for (String cloudFile : objectNowList) {
                            //  遍历云端文件，检查本地是否存在，不存在则删除
                            if (!nowFilePathList.contains(cloudFile)) {
                                System.out.println("本地不存在 " + cloudFile + " 文件，正在云端删除...");
                                s3.deleteObject(bucketName, cloudFile);
                                System.out.println("删除云端文件 " + cloudFile + " 成功！");
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            } catch (AmazonClientException e) {
                System.err.println(e.toString());
                System.exit(1);
            } finally {
                if (s3is != null) try {
                    s3is.close();
                } catch (IOException e) {
                }
                if (fos != null) try {
                    fos.close();
                } catch (IOException e) {
                }
            }

        }

    }

    //  获取指定目录下的所有文件路径
    public static List<String> getFilePathFromDir(String dir) throws IOException {
        List<String> list = new ArrayList<>();
        File baseFile = new File(dir);
        File[] files = baseFile.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    list.add((file.getCanonicalPath() + "\\"));
                    list.addAll(getFilePathFromDir(file.getCanonicalPath()));
                } else {
                    list.add(file.getCanonicalPath());
                }
            }
        }
        for (int i = 0; i < list.size(); ++i) {
            list.set(i, list.get(i).replace("\\", "/").replace(dir, ""));
        }

        return list;
    }

    //  获取指定目录下的所有文件
    public static List<File> getFilesFromDir(String dir) throws IOException {
        List<File> list = new ArrayList<>();
        File baseFile = new File(dir);
        File[] files = baseFile.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    list.add(file);
                    list.addAll(getFilesFromDir(file.getCanonicalPath()));
                } else {
                    list.add(file);
                }
            }
        }
        return list;
    }

    public static void upload_blocked(AmazonS3 s3, String bucketName, String localPath, File file) throws IOException {
        // Create a list of UploadPartResponse objects. You get one of these
        // for each part upload.
        //  为大文件建立上传分块表
        ArrayList<PartETag> partETags = new ArrayList<PartETag>();
        long contentLength = file.length();
        String uploadId = null;

        String keyName = localPath;
        long partSize_local = partSize;

        try {
            // Step 1: Initialize.
            //  初始化分块上传请求对象
            InitiateMultipartUploadRequest initRequest =
                    new InitiateMultipartUploadRequest(bucketName, keyName);
            uploadId = s3.initiateMultipartUpload(initRequest).getUploadId();
            System.out.format("Created upload ID was %s\n", uploadId);

            // Step 2: Upload parts.
            //  开始分块上传
            long filePosition = 0;
            for (int i = 1; filePosition < contentLength; i++) {
                // Last part can be less than 5 MB. Adjust part size.
                //  最后一个块可以小于设定的块大小
                partSize_local = Math.min(partSize_local, contentLength - filePosition);

                // Create request to upload a part.
                //  建立连接，上传块数据
                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(keyName)
                        .withUploadId(uploadId)
                        .withPartNumber(i)
                        .withFileOffset(filePosition)
                        .withFile(file)
                        .withPartSize(partSize_local);

                // Upload part and add response to our list.
                System.out.format("Uploading part %d\n", i);
                partETags.add(s3.uploadPart(uploadRequest).getPartETag());

                filePosition += partSize_local;
            }

            // Step 3: Complete.
            System.out.println("Completing upload");
            CompleteMultipartUploadRequest compRequest =
                    new CompleteMultipartUploadRequest(bucketName, keyName, uploadId, partETags);

            s3.completeMultipartUpload(compRequest);
        } catch (Exception e) {
            System.err.println(e.toString());
            if (uploadId != null && !uploadId.isEmpty()) {
                // Cancel when error occurred
                System.out.println("Aborting upload");
                s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, keyName, uploadId));
            }
            System.exit(1);
        }
        System.out.println("Done!");
    }

    public static void download_blocked(AmazonS3 s3, String bucketName,
                                        File file, String synlocalPath) throws IOException {
        String keyName = file.getCanonicalPath().
                replace("\\", "/").replace(synlocalPath, "");

        S3Object o = null;

        S3ObjectInputStream s3is = null;
        FileOutputStream fos = null;
        long partSize_local = partSize;

        try {
            // Step 1: Initialize.
            ObjectMetadata oMetaData = s3.getObjectMetadata(bucketName, keyName);
            final long contentLength = oMetaData.getContentLength();
            final GetObjectRequest downloadRequest =
                    new GetObjectRequest(bucketName, keyName);

            if (file.exists() && contentLength != file.length()) {
                fos = new FileOutputStream(file, true);

                // Step 2: Download parts.
                //  设置文件流追加写入的位置
                long filePosition = file.length();
                for (int i = (int)((file.length() / partSize) + 1) ; filePosition < contentLength; i++) {
                    // Last part can be less than 5 MB. Adjust part size.
                    partSize_local = Math.min(partSize_local, contentLength - filePosition);

                    // Create request to download a part.
                    downloadRequest.setRange(filePosition, filePosition + partSize_local);
                    o = s3.getObject(downloadRequest);

                    // download part and save to local file.
                    System.out.format("Downloading part %d\n", i);

                    filePosition += partSize_local + 1;
                    s3is = o.getObjectContent();
                    byte[] read_buf = new byte[64 * 1024];
                    int read_len = 0;
                    while ((read_len = s3is.read(read_buf)) > 0) {
                        fos.write(read_buf, 0, read_len);
                    }
                }
            } else {
                fos = new FileOutputStream(file);

                // Step 2: Download parts.
                long filePosition = 0;
                for (int i = 1; filePosition < contentLength; i++) {
                    // Last part can be less than 5 MB. Adjust part size.
                    partSize_local = Math.min(partSize_local, contentLength - filePosition);

                    // Create request to download a part.
                    downloadRequest.setRange(filePosition, filePosition + partSize_local);
                    o = s3.getObject(downloadRequest);

                    // download part and save to local file.
                    System.out.format("Downloading part %d\n", i);

                    filePosition += partSize_local + 1;
                    s3is = o.getObjectContent();
                    byte[] read_buf = new byte[64 * 1024];
                    int read_len = 0;
                    while ((read_len = s3is.read(read_buf)) > 0) {
                        fos.write(read_buf, 0, read_len);
                    }
                }
            }

            // Step 3: Complete.
            System.out.println("Completing download");

            System.out.format("save %s to %s\n", keyName, file.getCanonicalPath());
        } catch (Exception e) {
            System.err.println(e.toString());

            System.exit(1);
        } finally {
            if (s3is != null) try {
                s3is.close();
            } catch (IOException e) {
            }
            if (fos != null) try {
                fos.close();
            } catch (IOException e) {
            }
        }
        System.out.println("Done!");
    }

}

