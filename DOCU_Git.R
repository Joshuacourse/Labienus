
# 功能大全

# Ability to view the differences between two versions of your code.
# Ability to revert back to older versions of your code.
# Ability to review the history of your code.
# Ability to determine when a bug was introduced.
# Ability to experiment with new features without worrying about losing functioning code.
# Ability to track the ownership of files and changes applied to them.



# 原理
# When a Git repository is initiated, Git creates a hidden subdirectory within the folder associated with the R project.

# gitignore 排除的文件格式包括:
# local configuration files that specify a user's settings on a certain computer
# temporary files
# large data files
# binary files
# executables
# files with passwords



##  Adding files
# The act of adding does not affect the Git repository (i.e., the metadata that is stored about your files)

## Commit
# A commit takes a snapshot of your code at a specified point in time. Using a Git commit is like using anchors and other protection when climbing. If you’re crossing a dangerous rock face you want to make sure you’ve used protection to catch you if you fall


# 三个专有名词
# Stage: Is a file being tracked by Git?
# Status：
# ?? - Git does not know about that file
# M - The file has been modified
# A - The file has been added, i.e., we're ready to commit the changes to it
# D - The file has been deleted


# Commit 的时候有什么东西被储存呢？
#(1) the person that added/staged & committed the file(s)
#(2) a unique identifier, so that the particular version of your file(s) can always be retrieved
#(3) a specific file that knows about the modifications of the file(s)
#(4) a parent (i.e., the previous commit)
#(5) the commit message that was supplied by the user, intended to be a human-readable hint as to what was done to the files

# 如果我作了修改想要回去呢？
# Diff里面有一个Revert (Git actually stores and tracks changes to your file on disk (basically forever!))

# SHA才能恢复
# very commit has certain metadata attached to it, such as the commit message and a unique identifer (called an SHA key). The SHA key is important if you want to roll back to a previous commit, you can think of it as its permanent address.

# View File可以看到以前的记录

# 前面我们讲到commit 之前可以恢复之前的snapsht,但是commit 之后还能回到更之前的snapshot吗？
# 答案是可以！system("git checkout <SHA> <filename>")
# system("git checkout c9457c51 zhirui.R")
# Checking out an old file will change the current file back to the previous state
# checking out an entire working directory (git checkout <SHA>)
# Conversely, if you checked out the entire working directory without specifying a file, Git will pull up the entire directory at the point in time that you directed it to, but it will leave the current state of your project untouched