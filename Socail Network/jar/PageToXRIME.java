package Extrate;

import java.io.*;
import java.util.*;

public class PageToXRIME {
	public static String FileRead(String FilePath){
		String FileContent = "";
		String FileLine = "";
		try {
			BufferedReader NameReader = new BufferedReader(new InputStreamReader(new FileInputStream(FilePath),"gbk"));	
			while((FileLine = NameReader.readLine())!=null){//读文件		
				FileContent+=FileLine+"\n";;
			}
			NameReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return FileContent;
	}
	
	public static LinkedHashSet<String> FileToSet(String FileLine){
		LinkedHashSet<String> UserNameSet = new LinkedHashSet<String>();
		String[] UserLine=FileLine.split("\n");
		for(int i=0;i<UserLine.length;i++){
			String[] UserName=UserLine[i].trim().split(" ");
			for(int j=0;j<UserName.length;j++){
				if(UserName[j].trim().length()!=0)
					UserNameSet.add(UserName[j]);
			}
		}
		return UserNameSet;		
	}
	
	public static LinkedHashMap<String,String> SetToMap(LinkedHashSet<String> FileSet){
		LinkedHashMap<String, String> FileMap = new LinkedHashMap<String, String>();
		Object Obj[]=FileSet.toArray();
		for(int i=0;i<Obj.length;i++)
			FileMap.put((String) Obj[i], String.valueOf((i+1)));
		return FileMap;
	}
	
	public static List<String> FileToList(String FileLine){
		List<String> UserNameList = new ArrayList<String>();
		String[] UserName=FileLine.trim().split(" ");
		for(int i=0;i<UserName.length;i++){
			if(UserName[i].trim().length()!=0)
				UserNameList.add(UserName[i]);
		}
		return UserNameList;		
	}
	
	public static LinkedHashSet<String> UserNameSelect(String Leader,LinkedHashSet<String> UserNameSet,List<String> UserNameList,LinkedHashMap<String,String> UserNameMap){
		LinkedHashSet<String> UserNameSelect = new LinkedHashSet<String>();
		List<Integer> UserNameListNum = new ArrayList<Integer>();
		List<String> UserNameSubList = new ArrayList<String>();
		String SuperUser="0";
		UserNameSelect.add(SuperUser);
		for(int i=0;i<UserNameList.size();i++)
			if(!UserNameSet.contains(UserNameList.get(i)))
				UserNameListNum.add(i);
		for(int i=0;i<UserNameListNum.size()-1;i++){
			UserNameSubList=UserNameList.subList(UserNameListNum.get(i), UserNameListNum.get(i+1));
			for(int j=0;j<UserNameSubList.size();j++){
				if(UserNameSubList.get(j).equals(Leader)){
					if(UserNameSet.contains(UserNameSubList.get(j-1)))
						UserNameSelect.add(UserNameMap.get(UserNameSubList.get(j-1)));
				}
			}
		}
		return UserNameSelect;		
	}
	
	public static void WriteFile(String FileContent,String FileName){
		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(FileName),"gbk")));
			out.write(FileContent);
			out.flush();
			out.close();
		} catch (IOException e) {
		e.printStackTrace();
		}		
	}
	
	public static void main(String args[]) {
		String ReadFilePath="E:/SRTP.txt";
		String WriteFilePath="E:/11111.txt";
		String Result="0";
		LinkedHashSet<String> OtherUser=new LinkedHashSet<String>();
		String FileContent=FileRead(ReadFilePath);
		LinkedHashSet<String> UserNameSet=FileToSet(FileContent);
		LinkedHashMap<String,String> UserNameMap=SetToMap(UserNameSet);
		List<String> UserNameList=FileToList(FileContent);
//		System.out.println(FileContent);
		System.out.println(UserNameSet.size());
		System.out.println(UserNameList.size());
		Object Obj[]=UserNameSet.toArray();		
		for(int i=0;i<UserNameSet.size();i++)
			Result+=" "+(i+1);
		//System.out.println(Result);
		for(int i=0;i<Obj.length;i++){
			OtherUser=UserNameSelect(Obj[i].toString(),UserNameSet,UserNameList,UserNameMap);
			System.out.println((i+1)+"\t"+UserNameMap.get(Obj[i].toString())+" "+OtherUser);
			//if(OtherUser.size()!=0)
				Result+="\n"+UserNameMap.get(Obj[i].toString())+" "+OtherUser.toString().replace("[", "").replace("]", "").replace(",", " ");
		}
		//System.out.println(Result);
		WriteFile(Result,WriteFilePath);
	}
}
