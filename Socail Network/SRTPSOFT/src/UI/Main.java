package UI;

import java.awt.*;
import java.awt.event.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.regex.Pattern;

import javax.swing.*;
import javax.swing.event.*;
import javax.swing.text.html.*;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import Extrate.Downloader;
import Extrate.NetworkInformation;
import Extrate.PageInformation;

public class Main extends JFrame implements HyperlinkListener {
	private JButton BackButton;
	private JButton ForwardButton;
	private JTextField TextField;
	private JEditorPane EditorPane;
	private JTextArea TextArea;
	private JMenuBar MenuBar;
	private JPanel ButtonPanel = new JPanel();
	private JPanel AppendPanel = new JPanel();
	private ArrayList UrlList = new ArrayList();
	public Downloader d = new Downloader();
	
	public Main() {
		super("基于云计算平台的社会网络分析系统");
		setSize(1024, 768);
		addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				ActionExit();
			}
		});

		//菜单
		MenuBar = new JMenuBar();
		JMenu[] MenuArray=new JMenu[Information.MenuItem.length]; 
		for(int i=0;i<Information.MenuItem.length;i++)
			MenuArray[i]=new JMenu(Information.MenuItem[i].toString());
		MenuArray[0].setMnemonic(KeyEvent.VK_F);
		JMenuItem fileExitMenuItem = new JMenuItem("退出",KeyEvent.VK_X);
		fileExitMenuItem.addActionListener(new ActionListener() {
		    public void actionPerformed(ActionEvent e) {
		    	ActionExit();
		    }
		});
		MenuArray[0].add(fileExitMenuItem);
		for(int i=0;i<Information.MenuItem.length;i++)
		    MenuBar.add(MenuArray[i]);		     
		setJMenuBar(MenuBar);		
	     
		//按钮栏		     
		BackButton = new JButton("<<后退");
		BackButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				ActionBack();
		    }
		});
		BackButton.setEnabled(false);
		ButtonPanel.add(BackButton);		     
		     
		TextField = new JTextField(35);
		TextField.addKeyListener(new KeyAdapter() {
		    public void keyReleased(KeyEvent e) {
		    	if (e.getKeyCode() == KeyEvent.VK_ENTER) {
		    		ActionGo();
		    	}
		    }
		});
		ButtonPanel.add(TextField);
		     
		JButton GoButton = new JButton("打开");
		GoButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				ActionGo();
			}
		});
		ButtonPanel.add(GoButton);
		     
		ForwardButton = new JButton("前进 >>");
		ForwardButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				ActionForward();
			}
		});
		ForwardButton.setEnabled(false);
		ButtonPanel.add(ForwardButton);
		     
		//页面显示区
		EditorPane = new JEditorPane();
		EditorPane.setContentType("text/html");
		EditorPane.setEditable(false);
		EditorPane.addHyperlinkListener(this);
		     
		//处理结果区
		TextArea=new JTextArea();
		TextArea.setEditable(false);
		TextArea.setText("\t\t\t");
		
		//附加按钮区
		JButton[] ButtonArray=new JButton[Information.MenuItem.length-1];
		for(int i=0;i<Information.MenuItem.length-1;i++)
			ButtonArray[i]=new JButton(Information.MenuItem[i+1].toString());
		AppendPanel.setLayout(new GridLayout(1,4));
		for(int i=0;i<4;i++)
			AppendPanel.add(ButtonArray[i]);
		
		ButtonArray[0].addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				URL PageUrl;
				TextArea.setText("");
				try {
					PageUrl = new URL(TextField.getText());
					ShowArea(PageUrl);
				} catch (MalformedURLException e1) {
					e1.printStackTrace();
				}				
			}
		});
		
		ButtonArray[1].addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				PageInformation PI= new Extrate.PageInformation();
				String PageHtml=TextArea.getText();
				ArrayList<String> TitleList=PI.GetTitle(PageHtml,PI.PageTitle);
				ArrayList<String> TiebaList=PI.GetTieba(PageHtml,PI.PageTitle);
				ArrayList<String> NumList=PI.GetNum(PageHtml,PI.PageNum);
//		        System.out.println(PI.GetFirstTitle(PageHtml,PI.FirstTitle));
//		        System.out.println(PI.GetFirstNum(PageHtml,PI.FirstNum));
		        TextArea.setText("");
		        for(int i=0;i<TitleList.size();i++)
		        	TextArea.append((i+1)+"\t"+TitleList.get(i)+"\t"+TiebaList.get(i)+"\t"+NumList.get(i)+"\n");
			}
		});
		
		ButtonArray[2].addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				NetworkInformation NI= new Extrate.NetworkInformation();
				String PageHtml=TextArea.getText();
				TextArea.setText("");
				TextArea.append(NI.GetAuthor(PageHtml,NI.PageAuthor)+"\n");
				TextArea.append(NI.GetNum(PageHtml,NI.PageNum)+"\n");
				TextArea.append(NI.GetUserNameSet(PageHtml,NI.PageUserName).toString()+"\n");
//				TextArea.append(NI.GetUserName(PageHtml,NI.PageUserName).size());
				//TextArea.append(NI.Result(NI.PageRankMatrix(NI.GetUserNameSet(PageHtml,NI.PageUserName)))+"\n");
			}
		});
		
//		ButtonArray[3].addActionListener(new ActionListener() {
//			public void actionPerformed(ActionEvent e) {
//				String PageHtml=TextArea.getText();
//				try {
//					TextArea.setText(Segmenter.IKAnalyzer.WordStatic(PageHtml).toString());
//				} catch (Exception E) {
//					E.printStackTrace();
//				}
//			}
//		});

		getContentPane().setLayout(new BorderLayout());
		getContentPane().add(new JScrollPane(ButtonPanel), BorderLayout.NORTH);
		getContentPane().add(new JScrollPane(EditorPane), BorderLayout.CENTER);
		//getContentPane().add(new JScrollPane(Tree), BorderLayout.WEST);
		getContentPane().add(new JScrollPane(TextArea), BorderLayout.EAST);
		getContentPane().add(new JScrollPane(AppendPanel), BorderLayout.SOUTH);
	}

	//退出程序
	private void ActionExit() {
		System.exit(0);
	}
		   
	//后退
	private void ActionBack() {
		URL currentUrl = EditorPane.getPage();
		int pageIndex = UrlList.indexOf(currentUrl.toString());
		String Url=UrlList.get(pageIndex-1).toString();
		try {
			TextField.setText(Url);
			ShowPage(new URL(Url), false);			
		}
		catch (Exception e) {			
		}
	}

	//前进
	private void ActionForward() {
		URL currentUrl = EditorPane.getPage();
		int pageIndex = UrlList.indexOf(currentUrl.toString());
		System.out.println(pageIndex+"\t"+UrlList.size());
		String Url=UrlList.get(pageIndex+1).toString();		
		try {
			TextField.setText(Url);
			ShowPage(new URL(Url), false);			
		}
		catch (Exception e) {			
		}
	}
		
	//操作显示
	private void ActionGo() {
		URL verifiedUrl = VerifyUrl(TextField.getText());
		if (verifiedUrl != null) {			
			ShowPage(verifiedUrl,true);
			//ShowArea(verifiedUrl);
		} else {
			ShowError("该地址无效");
		}
	}
	
	//源码显示
	private void ShowArea(URL PageUrl){
		String Url=PageUrl.toString();
		String Html=d.getHTMLString(Url, false);
		TextArea.setText(Html);
	}
	
	//错误显示
	private void ShowError(String errorMessage) {
		JOptionPane.showMessageDialog(this, errorMessage,"Error", JOptionPane.ERROR_MESSAGE);
	}
	
	//URL验证
	private URL VerifyUrl(String Url) {
		if (!Url.toLowerCase().startsWith("http://"))
			return null;
		URL VerifiedUrl = null;
		try {
			VerifiedUrl = new URL(Url);
		} catch (Exception e) {
			return null;
		}
		return VerifiedUrl;
	}
		   
	//页面显示
	private void ShowPage(URL pageUrl, boolean addToList) {
		setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
	    try {
	    	//System.out.println(pageUrl);
	    	TextField.setText(pageUrl.toString());
	    	EditorPane.setPage(pageUrl);	    	
	    	if (addToList) {	    		
	    		UrlList.add(pageUrl.toString());
	    	}
	    	UpdateButtons();
	    }
	    catch (Exception e) {
	       //showError("浏览器无法显示该页面");
	    }
	    finally {
	    	setCursor(Cursor.getDefaultCursor());
	    }
	}
	
	//更新
	private void UpdateButtons() throws MalformedURLException {
		URL currentUrl = new URL(TextField.getText());
		int pageIndex = UrlList.indexOf(currentUrl.toString());
		//System.out.println(pageIndex+"\t"+UrlList.size());
		if (UrlList.size()<2) {
			BackButton.setEnabled(false);
			ForwardButton.setEnabled(false);
		}
		else{
			BackButton.setEnabled(pageIndex>0);
			ForwardButton.setEnabled(pageIndex<(UrlList.size()-1));
		}			
	}
		   
	//链接更新
	public void hyperlinkUpdate(HyperlinkEvent event) {
		HyperlinkEvent.EventType eventType = event.getEventType();
		if (eventType == HyperlinkEvent.EventType.ACTIVATED) {
			if (event instanceof HTMLFrameHyperlinkEvent) {
				HTMLFrameHyperlinkEvent linkEvent = (HTMLFrameHyperlinkEvent) event;
				HTMLDocument document = (HTMLDocument) EditorPane.getDocument();
				document.processHTMLFrameHyperlinkEvent(linkEvent);
			} else {
				ShowPage(event.getURL(),true);
			}
		}
	}
	
	//主程序
	public static void main(String[] args) {
		Main Frame = new Main();
		Frame.show();
	}
}
