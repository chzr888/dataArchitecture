package com.victop.tool.dataarchitecture;

import java.awt.event.MouseEvent;

import javax.swing.JFrame;
import javax.swing.JOptionPane;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.victop.tool.dataarchitecture.log.Log;

public class GUI {
	
	private static final long serialVersionUID = 1L;
	
	private static Logger logger = LoggerFactory.getLogger(GUI.class);
	private javax.swing.JFrame jframe = null;
	private javax.swing.JButton jButton1;
	private javax.swing.JLabel jLabel1;
	private javax.swing.JPanel jPanel1;
	private javax.swing.JPanel jPanel2;
	private javax.swing.JScrollPane jScrollPane1;
	private javax.swing.JTextArea jTextArea1;
	private javax.swing.JTextField jTextField1;

	public void initComponents(JFrame _jframe) {

		this.jframe = _jframe;

		jPanel1 = new javax.swing.JPanel();
		jScrollPane1 = new javax.swing.JScrollPane();
		jTextArea1 = new javax.swing.JTextArea();
		jPanel2 = new javax.swing.JPanel();
		jLabel1 = new javax.swing.JLabel();
		jTextField1 = new javax.swing.JTextField();
		jButton1 = new javax.swing.JButton();

		jPanel1.setBorder(javax.swing.BorderFactory.createTitledBorder("日志"));
		jPanel1.setToolTipText("");

		jTextArea1.setColumns(20);
		jTextArea1.setRows(5);
		jTextArea1.setLineWrap(true);
		jTextArea1.setWrapStyleWord(true);
		jScrollPane1.setViewportView(jTextArea1);

		javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(
				jPanel1);
		jPanel1.setLayout(jPanel1Layout);
		jPanel1Layout.setHorizontalGroup(jPanel1Layout.createParallelGroup(
				javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				javax.swing.GroupLayout.Alignment.TRAILING,
				jPanel1Layout.createSequentialGroup().addContainerGap()
						.addComponent(jScrollPane1).addContainerGap()));
		jPanel1Layout.setVerticalGroup(jPanel1Layout.createParallelGroup(
				javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				jPanel1Layout
						.createSequentialGroup()
						.addComponent(jScrollPane1,
								javax.swing.GroupLayout.DEFAULT_SIZE, 359,
								Short.MAX_VALUE).addContainerGap()));

		jPanel2.setBorder(javax.swing.BorderFactory.createTitledBorder(null,
				"数据架构", javax.swing.border.TitledBorder.DEFAULT_JUSTIFICATION,
				javax.swing.border.TitledBorder.DEFAULT_POSITION, null,
				new java.awt.Color(0, 0, 0)));

		jLabel1.setText("数据架构版本GUID:");

		jButton1.setText("转换");

		jButton1.addMouseListener(new java.awt.event.MouseAdapter() {
			public void mouseClicked(java.awt.event.MouseEvent evt) {
				jButton1MouseClicked(evt);
			}
		});

		javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(
				jPanel2);
		jPanel2.setLayout(jPanel2Layout);
		jPanel2Layout
				.setHorizontalGroup(jPanel2Layout
						.createParallelGroup(
								javax.swing.GroupLayout.Alignment.LEADING)
						.addGroup(
								jPanel2Layout
										.createSequentialGroup()
										.addContainerGap()
										.addComponent(jLabel1)
										.addPreferredGap(
												javax.swing.LayoutStyle.ComponentPlacement.RELATED)
										.addComponent(
												jTextField1,
												javax.swing.GroupLayout.PREFERRED_SIZE,
												551,
												javax.swing.GroupLayout.PREFERRED_SIZE)
										.addPreferredGap(
												javax.swing.LayoutStyle.ComponentPlacement.RELATED)
										.addComponent(
												jButton1,
												javax.swing.GroupLayout.PREFERRED_SIZE,
												83,
												javax.swing.GroupLayout.PREFERRED_SIZE)
										.addGap(0, 0, Short.MAX_VALUE)));
		jPanel2Layout
				.setVerticalGroup(jPanel2Layout
						.createParallelGroup(
								javax.swing.GroupLayout.Alignment.LEADING)
						.addGroup(
								jPanel2Layout
										.createSequentialGroup()
										.addContainerGap()
										.addGroup(
												jPanel2Layout
														.createParallelGroup(
																javax.swing.GroupLayout.Alignment.BASELINE)
														.addComponent(jLabel1)
														.addComponent(
																jTextField1,
																javax.swing.GroupLayout.PREFERRED_SIZE,
																javax.swing.GroupLayout.DEFAULT_SIZE,
																javax.swing.GroupLayout.PREFERRED_SIZE)
														.addComponent(jButton1))
										.addContainerGap(
												javax.swing.GroupLayout.DEFAULT_SIZE,
												Short.MAX_VALUE)));

		javax.swing.GroupLayout layout = new javax.swing.GroupLayout(
				this.jframe.getContentPane());
		this.jframe.getContentPane().setLayout(layout);
		layout.setHorizontalGroup(layout
				.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(
						layout.createSequentialGroup()
								.addContainerGap()
								.addGroup(
										layout.createParallelGroup(
												javax.swing.GroupLayout.Alignment.LEADING)
												.addComponent(
														jPanel2,
														javax.swing.GroupLayout.DEFAULT_SIZE,
														javax.swing.GroupLayout.DEFAULT_SIZE,
														Short.MAX_VALUE)
												.addComponent(
														jPanel1,
														javax.swing.GroupLayout.DEFAULT_SIZE,
														javax.swing.GroupLayout.DEFAULT_SIZE,
														Short.MAX_VALUE))
								.addContainerGap()));
		layout.setVerticalGroup(layout
				.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(
						layout.createSequentialGroup()
								.addContainerGap()
								.addComponent(jPanel2,
										javax.swing.GroupLayout.PREFERRED_SIZE,
										javax.swing.GroupLayout.DEFAULT_SIZE,
										javax.swing.GroupLayout.PREFERRED_SIZE)
								.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED)
								.addComponent(jPanel1,
										javax.swing.GroupLayout.DEFAULT_SIZE,
										javax.swing.GroupLayout.DEFAULT_SIZE,
										Short.MAX_VALUE).addContainerGap()));

	}

	protected void jButton1MouseClicked(MouseEvent evt) {
		String message = "";
		try {
			jButton1.setEnabled(false);
			String guidStr = jTextField1.getText();
			if(null != guidStr && !"".equalsIgnoreCase(guidStr) && !"null".equalsIgnoreCase(guidStr)){
				String [] args = guidStr.split(",");
				builder(args);
				message = "转换成功！";
			}else{
				message = "数据架构版本GUID不能为空，请输入！";
			}
		} catch (Exception e) {
			message = "转换失败,\n"+ e.getMessage();
			logger.error("{}",e);
		}finally{
			jButton1.setEnabled(true);
			JOptionPane.showMessageDialog(null, message, "提示",
					JOptionPane.INFORMATION_MESSAGE);
		}
	}

	private void builder(String[] args) throws Exception {
		if (args.length == 0) {
			throw new Exception("至少要传一下 schemaverguid ");
		}
		Log log = new Log(jTextArea1);
		String schemaverguid = args[0];
		BuilderHelper helper = new BuilderHelper();
		helper.setLog(jTextArea1);
		try {
			helper.init(schemaverguid);
			helper.initH2DB();
			try {
				log.println("<===============================================================>");
				log.println("开始转换！");
				// 插入数据库
				helper.h2Conn.setAutoCommit(false);
				for (int i = 0; i < args.length; i++) {
					helper.searchResource(args[i]);
					helper.batchConvert(args[i]);
				}
				helper.h2Conn.commit();
				log.println("");
				log.println("转换完成！");
			} catch (Exception e) {
				log.println("转换失败！");
				log.println(e.getMessage());
				helper.h2Conn.rollback();
				e.printStackTrace();
				throw e;
			}
			try {
				log.println("开始检查！");
				// 生效数据
				for (int i = 0; i < args.length; i++) {
					helper.batchActive(args[i]);
				}
				boolean b1 = true;
				// 查询生效失败资源
				for (int i = 0; i < args.length; i++) {
					if(helper.effectiveFailure(args[i])){
						b1 = false;
					}
				}
				if(b1){
					logger.info("生效成功!");
					log.println("");
					log.println("生效成功!");
				}else{
					logger.info("生效失败!");
					log.println("");
					log.println("生效失败!");
					throw new Exception("生效失败!");
				}
				log.println("检查完成！");
			} catch (Exception e) {
				throw e;
			}
		} catch (Exception e) {
			logger.error("{}",e);
			throw e;
		}finally{
			helper.closeConn();
		}
	}

	public static void main(String[] args) {
		try {
			// 数据原表测试
//			 String [] arr = {"024A8F9D-0184-A103-54EB-D0CA0D7314AC"};
			// 业务规则测试
//			 String [] arr = {"3989AF7B-3967-14F7-4F88-EA62DE8DDEC8"};
			// 主档
//			String[] arr = { "0673C8D3-8A75-677E-8E6B-F1A5CB1FE122" };
			//数据集
//			String [] arr = {"3989AF7B-3967-14F7-4F88-EA62DE8DDEC8"};
			//业务查询
//			String [] arr = {"3989AF7B-3967-14F7-4F88-EA62DE8DDEC8"};
			//数据模型
//			String [] arr = {"3989AF7B-3967-14F7-4F88-EA62DE8DDEC8"};
			//功能号
			String [] arr = {"3989AF7B-3967-14F7-4F88-EA62DE8DDEC8"};
			new GUI().builder(arr);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
