#ifndef ProductName
  #error ProductName must be supplied from branding.json
#endif
#ifndef ProductVersion
  #error ProductVersion must be supplied from branding.json
#endif
#ifndef Publisher
  #error Publisher must be supplied from branding.json
#endif
#ifndef UpgradeCode
  #error UpgradeCode must be supplied from branding.json
#endif
#ifndef BundleIdentifier
  #error BundleIdentifier must be supplied from branding.json
#endif
#ifndef PackageName
  #error PackageName must be supplied from branding.json
#endif
#ifndef ExecutableName
  #error ExecutableName must be supplied from branding.json
#endif
#ifndef StageDir
  #error StageDir must point to the assembled application directory
#endif
#ifndef OutputDir
  #error OutputDir must point to the installer output directory
#endif

[Setup]
AppId={#BundleIdentifier}-{#UpgradeCode}
AppName={#ProductName}
AppVersion={#ProductVersion}
AppPublisher={#Publisher}
DefaultDirName={autopf64}\{#ProductName}
DefaultGroupName={#ProductName}
DisableProgramGroupPage=yes
PrivilegesRequired=admin
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64
UninstallDisplayIcon={app}\{#ExecutableName}
SetupIconFile=..\..\assets\yt-save.ico
OutputDir={#OutputDir}
OutputBaseFilename={#PackageName}-{#ProductVersion}-x64-setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
Uninstallable=yes
CloseApplications=yes
RestartApplications=no

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; GroupDescription: "Additional shortcuts:"; Flags: unchecked

[Files]
Source: "{#StageDir}\*"; Excludes: ".yt-save-package-stage"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Registry]
Root: HKLM; Subkey: "Software\{#BundleIdentifier}"; ValueName: "InstalledByInno"; ValueType: dword; ValueData: 1; Flags: uninsdeletevalue

[Icons]
Name: "{autoprograms}\{#ProductName}"; Filename: "{app}\{#ExecutableName}"; WorkingDir: "{app}"
Name: "{autodesktop}\{#ProductName}"; Filename: "{app}\{#ExecutableName}"; WorkingDir: "{app}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#ExecutableName}"; Description: "Launch {#ProductName}"; Flags: postinstall nowait skipifsilent

[Code]
function InitializeSetup(): Boolean;
var
  InstalledByMSI: Cardinal;
begin
  Result := True;
  if RegQueryDWordValue(HKLM, 'Software\{#BundleIdentifier}', 'InstalledByMSI', InstalledByMSI) then
  begin
    MsgBox('Uninstall the MSI installation of Yt Save before installing the Inno Setup package.', mbError, MB_OK);
    Result := False;
  end;
end;
