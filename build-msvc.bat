@echo off
rem MSVC build: link against rustc's cdylib import library (firelite.dll.lib).
call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat" >NUL
cl /O2 /std:c++17 /EHsc /IC:\Dev\libs\firelite benchmark.cpp /link /LIBPATH:C:\Dev\libs\firelite\target\release firelite.dll.lib /OUT:benchmark-msvc.exe > cl-out.log 2>&1
echo CL_EXIT=%ERRORLEVEL%
