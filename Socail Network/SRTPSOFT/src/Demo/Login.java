package Demo;

import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Panel;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

public class Login extends JFrame {
	static Panel PanelUserName;
	static Panel PanelPassWord;
	static Panel PanelButton;
	
	static JButton ButtonLogin=new JButton("登录");
	static JButton ButtonCancel=new JButton("取消");
	static JButton ButtonHelp=new JButton("帮助");
	
	private JLabel UserNameLabel = new JLabel("用户名:");
	private JLabel PassWordLabel = new JLabel(" 密码  :");
	
	private JTextField UserNameText = new JTextField(20);
	private JPasswordField PassWordText = new JPasswordField(20);
	
	public Login() {
		PanelUserName=new Panel();
		PanelUserName.setLayout(new FlowLayout());
		PanelUserName.add(UserNameLabel);
		PanelUserName.add(UserNameText);
	  
		PanelPassWord=new Panel();
		PanelPassWord.setLayout(new FlowLayout());
		PanelPassWord.add(PassWordLabel);
		PanelPassWord.add(PassWordText);
	  
		PanelButton=new Panel();
		PanelButton.setLayout(new FlowLayout());
		PanelButton.add(ButtonLogin);
		PanelButton.add(ButtonCancel);
		PanelButton.add(ButtonHelp);
	  
		ButtonLogin.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String UserName = UserNameText.getText().trim();
				String PassWord = PassWordText.getText().trim();
				if(UserName.length()==0){
					JOptionPane.showMessageDialog(null,"请输入用户名!", "提示",JOptionPane.WARNING_MESSAGE);
					UserNameText.setText(null);
					PassWordText.setText(null);
				}
				else if(UserName.length()!=0 && PassWord.length()==0){
					JOptionPane.showMessageDialog(null,"请输入密码!", "提示",JOptionPane.WARNING_MESSAGE);
					PassWordText.setText(null);
				}
				else if(!UserName.equals("123")){
					JOptionPane.showMessageDialog(null,"不存在此用户!", "提示",JOptionPane.WARNING_MESSAGE);
					UserNameText.setText(null);
					PassWordText.setText(null);
				}
				else if(UserName.equals("123") && !PassWord.equals("123")){
					JOptionPane.showMessageDialog(null,"密码错误!", "提示",JOptionPane.WARNING_MESSAGE);
					PassWordText.setText(null);
				}
				else if(UserName.equals("123") && PassWord.equals("123")){
					PassWordText.setText(null);
					//System.exit(0);
					UI.Main UI = new UI.Main();
					UI.show();
				}
		    }
		});
		
		ButtonCancel.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				System.exit(0);
			}
		});
		
		ButtonHelp.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				JOptionPane.showMessageDialog(null,"请输入用户名及密码以使用该系统......", "提示",JOptionPane.INFORMATION_MESSAGE);
			}
		});
		
		setLayout(new GridLayout(3,1));
		add(PanelUserName);
		add(PanelPassWord);
		add(PanelButton);
		setTitle("登录");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setSize(300, 150);
		setLocation(300, 300);
		setVisible(true);
	}
	
	public static void main(String[] args) {
		Login Frame = new Login();
		Frame.show();
	}
}
