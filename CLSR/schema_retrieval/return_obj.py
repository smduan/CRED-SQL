from enum import Enum


class RetCodeEnum(str, Enum):
    Success = 0
    Failed = 10001
    Unknown = 500
    Partial = 1


class RetObj:
    def __init__(self, code=None, msg=None, data=None):
        if code is None:
            self.code = 0  
        else:
            self.code = code  
        if msg is None:
            self.msg = ""  
        else:
            self.msg = msg

        self.data = data  

    @classmethod
    def build_failed(cls, msg):
        return cls(RetCodeEnum.Failed, msg)

    @classmethod
    def build_success(cls, data=None):
        return cls(RetCodeEnum.Success, "", data)

    @classmethod
    def build_partial(cls, data=None):
        return cls(RetCodeEnum.Partial, "", data)

    @classmethod
    def build(cls, code, msg):
        return cls(code, msg)

    def is_success(self):
        return RetCodeEnum.Success == self.code

    def is_partial(self):
        return RetCodeEnum.Partial == self.code

