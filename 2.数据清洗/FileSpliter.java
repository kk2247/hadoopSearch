package util;

import java.awt.datatransfer.StringSelection;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author KGZ
 * @date 2018/12/23 8:54
 */
public class FileSpliter {

    private List<FileContent> fileContents;

    public FileSpliter() throws UnsupportedEncodingException {
        fileContents=new ArrayList<FileContent>();
        String path="C:\\Users\\22478\\Desktop\\data\\";
        for(int i=1;i<500;i++){
            String fileName=path+i+".txt";
            String content=readToString(fileName);
            String titleAndLink=content.split("<!DOCTYPE html>")[0];
            FileContent fileContent=new FileContent();
            fileContent.setUrl(titleAndLink.split("\\[")[0]);
            List<String> links=new ArrayList<String>();
            String[] link1=titleAndLink.split("\\[")[1].split(",");
            for(int j=0;j<link1.length;j++){
                links.add(link1[j]);
            }
            if(titleAndLink.split("\\[").length==3){
                String[] link2=titleAndLink.split("\\[")[2].split(",");
                for(int j=0;j<link2.length-1;j++){
                    links.add(link2[j]);
                }
                String s1=titleAndLink.split("\\[")[2].split("\\n")[1];
                String s2 = new String(s1.getBytes("ISO-8859-1"),"UTF-8");
                fileContent.setTitle(s2);
            }else if(titleAndLink.split("\\[").length==2){
                String s1=titleAndLink.split("\\[")[1].split("\\n")[1];
                String s2 = new String(s1.getBytes("ISO-8859-1"),"UTF-8");
                fileContent.setTitle(s2);
            }else{
                System.out.print(i);
            }
            fileContent.setLinks(links);

            String con=Html2Text(content.split("<!DOCTYPE html>")[1]);
            String con2 = new String(con.getBytes("ISO-8859-1"),"UTF-8");
            fileContent.setContent(con2);
            fileContents.add(fileContent);
        }


    }

    public List<FileContent> getFileContents() {
        return fileContents;
    }

    public void setFileContents(List<FileContent> fileContents) {
        this.fileContents = fileContents;
    }

    private String readToString(String fileName) {
        String encoding = "ISO-8859-1";
        File file = new File(fileName);
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            return new String(filecontent, encoding);
        } catch (UnsupportedEncodingException e) {
            System.err.println("The OS does not support " + encoding);
            e.printStackTrace();
            return null;
        }
    }

    private String Html2Text(String inputString) {
        String htmlStr = inputString; // 含html标签的字符串
        String textStr = "";
        java.util.regex.Pattern p_script;
        java.util.regex.Matcher m_script;
        java.util.regex.Pattern p_style;
        java.util.regex.Matcher m_style;
        java.util.regex.Pattern p_html;
        java.util.regex.Matcher m_html;
        try {
            String regEx_script = "<[\\s]*?script[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?script[\\s]*?>"; // 定义script的正则表达式{或<script[^>]*?>[\\s\\S]*?<\\/script>
            String regEx_style = "<[\\s]*?style[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?style[\\s]*?>"; // 定义style的正则表达式{或<style[^>]*?>[\\s\\S]*?<\\/style>
            String regEx_html = "<[^>]+>"; // 定义HTML标签的正则表达式
            p_script = Pattern.compile(regEx_script, Pattern.CASE_INSENSITIVE);
            m_script = p_script.matcher(htmlStr);
            htmlStr = m_script.replaceAll(""); // 过滤script标签
            p_style = Pattern.compile(regEx_style, Pattern.CASE_INSENSITIVE);
            m_style = p_style.matcher(htmlStr);
            htmlStr = m_style.replaceAll(""); // 过滤style标签
            p_html = Pattern.compile(regEx_html, Pattern.CASE_INSENSITIVE);
            m_html = p_html.matcher(htmlStr);
            htmlStr = m_html.replaceAll(""); // 过滤html标签
            textStr = htmlStr;
        } catch (Exception e) {System.err.println("Html2Text: " + e.getMessage()); }
        //剔除空格行
        textStr=textStr.replaceAll("[ ]+", " ");
        textStr=textStr.replaceAll("(?m)^\\s*$(\\n|\\r\\n)", "");
        return textStr;// 返回文本字符串
    }

}
