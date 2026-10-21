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
PrivilegesRequiredOverridesAllowed=dialog commandline
UsePreviousPrivileges=yes
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
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
Root: HKA; Subkey: "Software\{#BundleIdentifier}"; ValueName: "InstalledByInno"; ValueType: dword; ValueData: 1; Flags: uninsdeletevalue

[Icons]
Name: "{autoprograms}\{#ProductName}"; Filename: "{app}\{#ExecutableName}"; WorkingDir: "{app}"
Name: "{autodesktop}\{#ProductName}"; Filename: "{app}\{#ExecutableName}"; WorkingDir: "{app}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#ExecutableName}"; Description: "Launch {#ProductName}"; Flags: postinstall nowait skipifsilent

[Code]
function ReadInstallerMarker(RootKey: Integer; ValueName: String; var Found: Boolean): Boolean;
var
  MarkerValue: Cardinal;
begin
  Found := RegValueExists(RootKey, 'Software\{#BundleIdentifier}', ValueName);
  if not Found then
  begin
    Result := True;
    Exit;
  end;

  if not RegQueryDWordValue(RootKey, 'Software\{#BundleIdentifier}', ValueName, MarkerValue) then
  begin
    MsgBox('A Yt Save installer marker has an unexpected registry type. Resolve the existing installation before continuing.', mbError, MB_OK);
    Result := False;
    Exit;
  end;

  if MarkerValue <> 1 then
  begin
    MsgBox('A Yt Save installer marker has an unexpected value. Resolve the existing installation before continuing.', mbError, MB_OK);
    Result := False;
    Exit;
  end;
  Result := True;
end;

function InitializeSetup(): Boolean;
var
  MsiUser, MsiMachine, InnoUser, InnoMachine: Boolean;
begin
  Result := True;
  if not ReadInstallerMarker(HKCU, 'InstalledByMSI', MsiUser) or
     not ReadInstallerMarker(HKLM, 'InstalledByMSI', MsiMachine) or
     not ReadInstallerMarker(HKCU, 'InstalledByInno', InnoUser) or
     not ReadInstallerMarker(HKLM, 'InstalledByInno', InnoMachine) then
  begin
    Result := False;
    Exit;
  end;

  if MsiUser or MsiMachine then
  begin
    MsgBox('Uninstall the MSI installation of Yt Save before installing the Inno Setup package.', mbError, MB_OK);
    Result := False;
    Exit;
  end;

  if InnoUser and InnoMachine then
  begin
    MsgBox('Yt Save Inno Setup markers exist in both the current-user and machine registry hives. Resolve the existing installations before continuing.', mbError, MB_OK);
    Result := False;
    Exit;
  end;

  if InnoUser and IsAdminInstallMode then
  begin
    MsgBox('Yt Save is installed for the current user. Select the current-user install mode to update it.', mbError, MB_OK);
    Result := False;
    Exit;
  end;

  if InnoMachine and not IsAdminInstallMode then
  begin
    MsgBox('Yt Save is installed for all users. Select the administrative install mode to update it.', mbError, MB_OK);
    Result := False;
  end;
end;
