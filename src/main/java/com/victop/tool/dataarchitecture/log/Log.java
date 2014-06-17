package com.victop.tool.dataarchitecture.log;

import javax.swing.JTextArea;

public class Log {
	
	public JTextArea jTextArea1 = null;
	
	public Log(){
		
	}
	public Log(JTextArea _jTextArea1){
		this.jTextArea1 = _jTextArea1;
	}
	
	public void println(String msg) {
		if(null == jTextArea1){
			return;
		}
		jTextArea1.append(msg);
		jTextArea1.append("\n");
		jTextArea1.paintImmediately(jTextArea1.getBounds());
	}
		
	public void println(String msg,boolean b) {
		if(null == jTextArea1){
			return;
		}
		jTextArea1.append(msg);
		if (b) {
			jTextArea1.append("\n");
		}
		jTextArea1.paintImmediately(jTextArea1.getBounds());
	}
	
	public void println(String format, Object... arguments) {
		if(null == jTextArea1){
			return;
		}
		String msg = format;
		for (int i = 0; i < arguments.length; i++) {
			String str = reStringByNull(arguments[i]);
			msg = msg.replace("{"+i+"}",str);
		}
		jTextArea1.append(msg);
		jTextArea1.append("\n");
		jTextArea1.paintImmediately(jTextArea1.getBounds());
	}
	
	public String reStringByNull(Object data) {
		if (data == null) {
			return "";
		} else {
			return data.toString();
		}
	}
	
}
