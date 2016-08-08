package Extrate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NetworkInformation {
	public final static Pattern PageAuthor = Pattern.compile("author:\"[^\"]*\"", Pattern.CASE_INSENSITIVE);
	public final static Pattern PageUserName = Pattern.compile("username=\"[^\"]*\" class=", Pattern.CASE_INSENSITIVE);
	public final static Pattern PageNum = Pattern.compile("共有<span class=\"red\">[1-9]*</span>页", Pattern.CASE_INSENSITIVE);

	public static String GetAuthor(String Html, Pattern Author){
		String AuthorTemp="";
		String AuthorResult="";
		int StartAuthorIndex=0;
		int EndAuthorIndex=0;
		Matcher AuthorRegex=Author.matcher(Html);
		if(AuthorRegex.find()){
			AuthorTemp=AuthorRegex.group().toString();
			StartAuthorIndex=AuthorTemp.indexOf("\"")+1;
			EndAuthorIndex=AuthorTemp.length()-1;
			AuthorResult=AuthorTemp.substring(StartAuthorIndex, EndAuthorIndex);
		}
		return AuthorResult;		
	}
	
	public static String GetNum(String Html, Pattern Num){
		String NumTemp="";
		String NumResult="";
		int StartNumIndex=0;
		int EndNumIndex=0;
		Matcher NumRegex=Num.matcher(Html);
		if(NumRegex.find()){
			NumTemp=NumRegex.group().toString();
			StartNumIndex=NumTemp.indexOf(">")+1;
			EndNumIndex=NumTemp.indexOf("</");
			NumResult=NumTemp.substring(StartNumIndex, EndNumIndex);
		}
		return NumResult;		
	}
	
	public static ArrayList<String> GetUserNameList(String Html, Pattern UserName){
		String UserNameTemp="";
		ArrayList<String> UserNameResult=new ArrayList<String>();
		int StartUserNameIndex=0;
		int EndUserNameIndex=0;
		Matcher UserNameRegex=UserName.matcher(Html);
		while(UserNameRegex.find()){
			UserNameTemp=UserNameRegex.group().toString();
			StartUserNameIndex=UserNameTemp.indexOf("\"")+1;
			EndUserNameIndex=UserNameTemp.indexOf(" ")-1;
			UserNameResult.add(UserNameTemp.substring(StartUserNameIndex, EndUserNameIndex));
		}
		return UserNameResult;		
	}
	
	public static LinkedHashSet<String> GetUserNameSet(String Html, Pattern UserName){
		String UserNameTemp="";
		LinkedHashSet<String> NameResult=new LinkedHashSet<String>();
		int StartUserNameIndex=0;
		int EndUserNameIndex=0;
		Matcher UserNameRegex=UserName.matcher(Html);
		while(UserNameRegex.find()){
			UserNameTemp=UserNameRegex.group().toString();
			StartUserNameIndex=UserNameTemp.indexOf("\"")+1;
			EndUserNameIndex=UserNameTemp.indexOf(" ")-1;
			NameResult.add(UserNameTemp.substring(StartUserNameIndex, EndUserNameIndex));
		}
		return NameResult;		
	}
	
	public static int[][] PageRankMatrix(ArrayList<String> NameList){
		HashSet<String> PageRankSet=new HashSet<String>();
		for(int i=0;i<NameList.size();i++)
			PageRankSet.add(NameList.get(i));
		int Size=PageRankSet.size();
		int Key=0;
		int[][] Matrix=new int[Size][Size];
		HashMap<String,Integer> NameMap=new HashMap<String,Integer>();
		Iterator iterator = PageRankSet.iterator();
		while (iterator.hasNext()) {
			String Name = iterator.next().toString();
			NameMap.put(Name, Key++);
		}			
		for(int i=0;i<NameList.size()-1;i++)
			Matrix[NameMap.get(NameList.get(i))][NameMap.get(NameList.get(i+1))]=1;
		return Matrix;
	}
	
	public static String Result(int[][] Matrix){
		String MatrixString="";
		int Size=Matrix.length;
		for(int i=0;i<Size;i++){
			MatrixString+=(i+1);
			for(int j=0;j<Size;j++){
				if(Matrix[i][j]==1){
					MatrixString+=" "+(j+1);
				}
			}	
			MatrixString+="\n";
		}
		return MatrixString;
	}
	
	public static void main(String args[]) {
		String userAgent = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)";
		String urlBase = "http://tieba.baidu.com/p/";
		String url = "http://tieba.baidu.com/p/2816457522";        
        Downloader d = new Downloader();
        String htmlStr = d.getHTMLString(url, false);
        
//        String url2 = "http://tieba.baidu.com/p/2813974937?pn=2";
//        String htmlStr2 = d.getHTMLString(url2, 500, 5000, userAgent, 3, false);
//        String htmlStr=htmlStr1+htmlStr2;
        
        //System.out.println(htmlStr);
        System.out.println(GetAuthor(htmlStr,PageAuthor));
        System.out.println(GetNum(htmlStr,PageNum));
        System.out.println(GetUserNameSet(htmlStr,PageUserName));
        System.out.println(GetUserNameSet(htmlStr,PageUserName).size());
        //System.out.println(Result(PageRankMatrix(GetUserName(htmlStr,PageUserName))));
	}
}
