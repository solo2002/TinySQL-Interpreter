

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.JFileChooser;
import javax.swing.JTextField;
import javax.swing.JButton;
import javax.swing.JLabel;

import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import javax.swing.JTextArea;
public class User extends JFrame implements MouseListener{

	private JPanel contentPane;
	private JTextField SQLQuery;
	private JTextField InputFile;
	public String fileName;
	public File file = null;
	//public OpenFile of;
	public String path;
	public static TinyParser tp;
	public User() 
	{
		final JFrame frame = new JFrame("Tiny SQL Parser");
		//setTitle("Tiny SQL Parser");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 300);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(null);
		
		JLabel lblPleaseEnterSql = new JLabel("Please enter SQL query or select input file");
		lblPleaseEnterSql.setFont(new Font("Times New Roman", Font.BOLD, 15));
		lblPleaseEnterSql.setBounds(66, 76, 317, 14);
		contentPane.add(lblPleaseEnterSql);
		SQLQuery = new JTextField();
		
		SQLQuery.setBounds(65, 118, 188, 20);
		contentPane.add(SQLQuery);
		SQLQuery.setColumns(10);
		
		InputFile = new JTextField();
		InputFile.setBounds(66, 153, 185, 20);
		contentPane.add(InputFile);
		InputFile.setColumns(10);
		
		JButton btnSubmit = new JButton("Submit");
		btnSubmit.setBounds(259, 117, 108, 23);
		contentPane.add(btnSubmit);
		
		tp = new TinyParser();
		
		JButton btnSelectFile = new JButton("Select file");
		JFileChooser fileChooser = new JFileChooser();
		btnSubmit.addMouseListener(this); 
		btnSelectFile.addActionListener(new ActionListener() 
		{
		      public void actionPerformed(ActionEvent ae) 
		      {
		    	  if(fileChooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION)
			  		{//get file
			  			java.io.File file = fileChooser.getSelectedFile();
			  			try {
							Scanner input = new Scanner(file);
							
							while (input.hasNextLine())
							{
								String str = input.nextLine();
								tp.parser(str);
							}
						    input.close();
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
			  		}
		    	 else 
		  			System.out.println("No file was selected");
		    	  //textArea.setText(of.sb.toString());
		      }
		    });	
		btnSelectFile.setBounds(259, 151, 108, 23);
		contentPane.add(btnSelectFile);
	}
	
	public void mouseClicked(MouseEvent arg0) 
	{
		String str = SQLQuery.getText();
		tp.parser(str);
	}
	
	@Override
	public void mouseEntered(MouseEvent arg0) {
		// TODO Auto-generated method stub
	}
	@Override
	public void mouseExited(MouseEvent e) {
		// TODO Auto-generated method stub
	}
	@Override
	public void mousePressed(MouseEvent e) {
		// TODO Auto-generated method stub	
	}
	@Override
	public void mouseReleased(MouseEvent e) {
		// TODO Auto-generated method stub
	}
	
	public static void main(String[] args) 
	{
		User user = new User();
		user.setVisible(true);
	}
}
