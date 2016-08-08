package Extrate;

import java.io.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageInformation {
	
	public final static Pattern FirstTitle = Pattern.compile("<a class=\"th_tit\" id=\"topic_post_title\" target=\"_blank\" href=\"http://tieba.baidu.com/p/[0-9]*\" title=\"[^\"]*\">", Pattern.CASE_INSENSITIVE);
	public final static Pattern FirstNum = Pattern.compile("\"[0-9]*个回复\"", Pattern.CASE_INSENSITIVE);
	
	public final static Pattern PageTitle = Pattern.compile("<a href=\"/p/[0-9]*\" title=\"[^\"]*\"", Pattern.CASE_INSENSITIVE);
	public final static Pattern PageNum = Pattern.compile("<div class=\"threadlist_rep_num\" title=\"回复\">[0-9]*", Pattern.CASE_INSENSITIVE);
	
	public static String GetFirstTitle(String Html, Pattern Title){
		String TitleTemp="";
		String TitleResult="";
		int StartTitleIndex=0;
		int EndTitleIndex=0;
		Matcher TitleRegex=Title.matcher(Html);
		if(TitleRegex.find()){
			TitleTemp=TitleRegex.group().toString();
			StartTitleIndex=TitleTemp.indexOf("title=\"")+7;
			EndTitleIndex=TitleTemp.length()-2;
			TitleResult=TitleTemp.substring(StartTitleIndex, EndTitleIndex);
		}
		return TitleResult;		
	}
	
	public static String GetFirstNum(String Html, Pattern Num){
		String NumTemp="";
		String NumResult="";
		int StartNumIndex=0;
		int EndNumIndex=0;
		Matcher NumRegex=Num.matcher(Html);
		if(NumRegex.find()){
			NumTemp=NumRegex.group().toString();
			StartNumIndex=NumTemp.indexOf("\"")+1;
			EndNumIndex=NumTemp.indexOf("个");
			NumResult=NumTemp.substring(StartNumIndex, EndNumIndex);
		}
		return NumResult;		
	}
	
	public static ArrayList<String> GetTitle(String Html, Pattern Title){
		String TitleTemp="";
		ArrayList<String> TitleResult=new ArrayList<String>();
		int StartTitleIndex=0;
		int EndTitleIndex=0;
		Matcher TitleRegex=Title.matcher(Html);
		while(TitleRegex.find()){
			TitleTemp=TitleRegex.group().toString();
			StartTitleIndex=TitleTemp.indexOf("title=\"")+7;
			EndTitleIndex=TitleTemp.length()-1;
			TitleResult.add(TitleTemp.substring(StartTitleIndex, EndTitleIndex));
		}
		return TitleResult;		
	}
	
	public static ArrayList<String> GetTieba(String Html, Pattern Title){
		String TitleTemp="";
		ArrayList<String> TiebaResult=new ArrayList<String>();
		int StartTiebaIndex=0;
		int EndTiebaIndex=0;
		Matcher TitleRegex=Title.matcher(Html);
		while(TitleRegex.find()){
			TitleTemp=TitleRegex.group().toString();
			StartTiebaIndex=TitleTemp.indexOf("href=")+9;
			EndTiebaIndex=TitleTemp.indexOf("title")-2;
			TiebaResult.add(TitleTemp.substring(StartTiebaIndex, EndTiebaIndex));
		}
		return TiebaResult;		
	}
	
	public static ArrayList<String> GetNum(String Html, Pattern Num){
		String NumTemp="";
		ArrayList<String> NumResult=new ArrayList<String>();
		int StartNumIndex=0;
		int EndNumIndex=0;
		Matcher NumRegex=Num.matcher(Html);
		while(NumRegex.find()){
			NumTemp=NumRegex.group().toString();
			StartNumIndex=NumTemp.indexOf(">")+1;
			EndNumIndex=NumTemp.length();
			NumResult.add(NumTemp.substring(StartNumIndex, EndNumIndex));
		}
		return NumResult;		
	}
	
	public static int FileLength(String Html,String FilePath){
		int Length=Html.length();
		try {
			FileWriter fwresult=new FileWriter(FilePath, false);
			BufferedWriter bwsent=new BufferedWriter(fwresult);
			bwsent.write(Html);
			bwsent.newLine();
			bwsent.flush();
			bwsent.close();				
		} catch (IOException e) {
			e.printStackTrace();
		}		
		return Length;
	}
	
	public static String SetToLine(String SetString){
		String Line="";
		String Result="";
		Line=SetString.substring(1, SetString.length()-1);
		String[] UserList=Line.split(",");
		for(int i=0;i<UserList.length;i++)
			Result+=UserList[i]+" ";
		return Result;
	}
	
	public static void main(String args[]) {
		String userAgent = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)";
		String urlBase = "http://tieba.baidu.com/p/";
		String url = "http://tieba.baidu.com/f?kw=%CE%F7%C4%CF%BD%BB%CD%A8%B4%F3%D1%A7&tp=0&pn=150";        
        Downloader d = new Downloader();
        String htmlStr = d.getHTMLString(url, false);
        String HtmlList="";
//        System.out.println(htmlStr);
        ArrayList<String> TitleList=GetTitle(htmlStr,PageTitle);
        ArrayList<String> TiebaList=GetTieba(htmlStr,PageTitle);
        ArrayList<String> NumList=GetNum(htmlStr,PageNum);
        //System.out.println(GetFirstTitle(htmlStr,FirstTitle));
        //System.out.println(GetFirstNum(htmlStr,FirstNum));
        //for(int i=0;i<TitleList.size();i++){
        	//System.out.println((i+1)+"\t"+TitleList.get(i)+"\t"+TiebaList.get(i)+"\t"+NumList.get(i));
        for(int i=0;i<TiebaList.size();i++){
        	//System.out.println((i+1)+"\t"+TitleList.get(i)+"\t"+TiebaList.get(i)+"\t"+NumList.get(i));
        	HtmlList=d.getHTMLString(urlBase+TiebaList.get(i), false);
        	if(Integer.valueOf(NumList.get(i))>50) {
        		for(int n=2;n<Integer.valueOf(NumList.get(i))/50+1;n++)
        		HtmlList+=d.getHTMLString(urlBase+TiebaList.get(i)+"?pn="+n, false);
        	}        	
        	//System.out.println((i+1)+"\t"+TitleList.get(i)+"\t"+NumList.get(i)+"\t"+NetworkInformation.GetUserNameSet(HtmlList,NetworkInformation.PageUserName));
        	System.out.println(SetToLine(NetworkInformation.GetUserNameSet(HtmlList,NetworkInformation.PageUserName).toString()));
        }
//        System.out.println(FileLength(htmlStr,"E:/123.txt"));
	}
}

