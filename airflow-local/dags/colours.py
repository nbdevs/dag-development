"""This module is responsible for text colour manipulation"""
from __future__ import annotations
from abc import ABC, abstractmethod

class AbstractColours(ABC):
    """ Colour interface for formatting text and inputs."""

    @abstractmethod
    def getBold(self):
        pass

    @abstractmethod
    def getEnd(self):
        pass

    @abstractmethod
    def getRed(self):
        pass

    @abstractmethod
    def getGreen(self):
        pass

    @abstractmethod
    def getOrange(self):
        pass

    @abstractmethod
    def getYellow(self):
        pass

    @abstractmethod
    def getCyan(self):
        pass

    @abstractmethod
    def getPurple(self):
        pass

    @abstractmethod
    def getLG(self):
        pass

class Colours(AbstractColours):
    '''Colours class:
    Reset all colors with colours.endFont
    '''
    endFont = '\033[0m'
    flashing = "\33[5m"
    boldFont = '\033[01m'
    disableFont = '\033[02m'
    underlineFont = '\033[04m'
    reverseFont = '\033[07m'
    strikeThroughFont = '\033[09m'
    invisibleFont = '\033[08m'
    italicFont = "\33[3m"
    blackFont = '\033[30m'
    redFont = '\033[31m'
    greenFont = '\033[32m'
    orangeFont = '\033[33m'
    blueFont = '\033[34m'
    purpleFont = '\033[35m'
    cyanFont = '\033[36m'
    lightGreyFont = '\033[37m'
    darkGreyFont = '\033[90m'
    lightRedFont = '\033[91m'
    yellowFont = '\033[93m'
    lightBlueFont = '\033[94m'
    pinkFont = '\033[95m'
    lightCyanFont = '\033[96m'

    def getBold(self):
        return self.boldFont

    def getEnd(self):
        return self.endFont

    def getRed(self):
        return self.redFont
    
    def getYellow(self):
        return self.yellowFont

    def getBlue(self):
        return self.blueFont

    def getGreen(self):
        return self.greenFont

    def getOrange(self):
        return self.orangeFont

    def getCyan(self):
        return self.cyanFont

    def getPurple(self):
        return self.purpleFont

    def getLG(self):
        return self.lightGreyFont

    class bg(AbstractColours):
        """Subclass for backgrounds. """
        blackBG = '\033[40m'
        redBG = '\033[41m'
        greenBG = '\033[42m'
        orangeBG = '\033[43m'
        blueBG = '\033[44m'
        purpleBG = '\033[45m'
        cyanBG = '\033[46m'
        lightGreyBG = '\033[47m'

        def getRed(self):
            return self.redBG

        def getGreen(self):
            return self.greenBG

        def getOrange(self):
            return self.orangeBG

        def getCyan(self):
            return self.cyanBG

        def getPurple(self):
            return self.purpleBG

        def getLG(self):
            return self.lightGreyBG
