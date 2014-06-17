package com.victop.tool.dataarchitecture;

import java.awt.Dimension;
import java.awt.Toolkit;
import javax.swing.JFrame;

/**
 * 测试
 * 
 * @author chenzr
 * 
 */
public class Main {

	private static void createAndShowGUI() {
		// Create and set up the window.
		JFrame frame = new JFrame("数据架构转换工具");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		// Set up the content pane.
		GUI gui = new GUI();
		gui.initComponents(frame);
		// Display the window.
		frame.pack();
		frame.setVisible(true);
		// 获取屏幕
		Dimension dimension = Toolkit.getDefaultToolkit().getScreenSize();
		// 要设置的组件大小
		int width = 830;
		int height = 600;
		// 设置为居中显示
		frame.setBounds((dimension.width - width) / 2,
				(dimension.height - height) / 2, width, height);
	}

	public static void main(String[] args) {
		try {

			javax.swing.SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					createAndShowGUI();
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
