package com.victop.tool.dataarchitecture.layout;

import java.awt.Container;
import java.awt.HeadlessException;
import javax.swing.GroupLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JTextField;

public class GroupLayoutDemo extends JFrame {
	/**
	 * 该类直接在构造方法里初始化GroupLayout，只为了方便演示
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * @throws HeadlessException
	 */
	public GroupLayoutDemo() throws HeadlessException {
		// 创建GroupLayout ---- //GroupLayout
		// 创建的时候需要给定一个Container，基本上所有的swing的轻量级组件都间接继承自Container
		Container c = getContentPane();
		GroupLayout layout = new GroupLayout(c);

		// 初始化一些组件
		JButton b1 = new JButton("button 1");
		JButton b2 = new JButton("button 2");
		JTextField text = new JTextField("Text Field");

		// GroupLayout
		// 顾名思义，就是通过Group来组织页面，打个不恰当的比喻，像是Html里的大<Table>套小<table>,而,GroupLayout也正是大Group套小Group
		// 对于GroupLayout来说,需要从两个方向对组件进行分组: 水平方向 和 垂直方向,某一个方向上又有两种排列方式,连续排列 和
		// 平行排列,这样就可以将组件固定到面板的某个位置上了.

		// 水平
		GroupLayout.SequentialGroup hsg = layout.createSequentialGroup(); // 水平方向上
																			// 创建
																			// 连续组
		hsg.addComponent(b1); // b1
		hsg.addComponent(b2); // b2
		GroupLayout.ParallelGroup hpg = layout
				.createParallelGroup(GroupLayout.Alignment.CENTER); // 水平方向上 创建
																	// 平行组
		hpg.addComponent(text).addGroup(hsg); // text and hsg(连续组)
		layout.setHorizontalGroup(hpg); // 设置到水平方向上
		// 垂直
		GroupLayout.ParallelGroup vpg = layout.createParallelGroup(); // 垂直方向上
																		// 创建
																		// 平行组
		vpg.addComponent(b1); // b1
		vpg.addComponent(b2); // b2
		GroupLayout.SequentialGroup vsg = layout.createSequentialGroup(); // 垂直方向上
																			// 创建
																			// 连续组
		vsg.addComponent(text).addGroup(vpg); // text and vpg(平行组)
		layout.setVerticalGroup(vsg); // 设置到垂直方向上

		this.setLayout(layout); //
		// other
		this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		pack();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		GroupLayoutDemo demo = new GroupLayoutDemo();
		demo.setVisible(true);
	}
}