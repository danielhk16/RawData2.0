#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 16 10:48:44 2018

@author: xingershang
"""

from pymouse import PyMouse
from pykeyboard import PyKeyboard
import numpy as np
import threading
import time


m = PyMouse()
k = PyKeyboard()

x_dim, y_dim = m.screen_size()
m.click(x_dim/2, y_dim/2, 1)





def mouse_move():
    global t
    randx = np.random.randint(1,10)
    randy = np.random.randint(1,10)
    m.position()#获取当前坐标的位置
    m.move(x_dim/randx,y_dim/randy)#鼠标移动到xy位置


    t=threading.Timer(10,mouse_move)
    t.start()
t=threading.Timer(5,mouse_move)
t.start()








t.cancel()




